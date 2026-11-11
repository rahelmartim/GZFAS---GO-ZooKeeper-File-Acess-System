// A small Chrome driver for the browser tests. It uses the Chrome DevTools
// Protocol over the WebSocket of Node.js, so it needs no libraries.

import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { extname, join, normalize, resolve } from "node:path";

const ROOT = resolve(process.env.SITE_DIR ?? resolve(import.meta.dirname, "..", "..", "site"));
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webmanifest": "application/manifest+json",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

/** Finds Chrome or Chromium, or returns null. */
export function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ];
  return candidates.find((path) => path && existsSync(path)) ?? null;
}

/** Serves the app folder on a free port. */
export function serveApp() {
  const server = createServer((request, response) => {
    const path = decodeURIComponent(new URL(request.url, "http://x").pathname);
    const file = normalize(join(ROOT, path === "/" ? "index.html" : path));
    if (!file.startsWith(ROOT) || !existsSync(file)) {
      response.writeHead(404).end("not found");
      return;
    }
    response.writeHead(200, { "Content-Type": TYPES[extname(file)] ?? "application/octet-stream" });
    response.end(readFileSync(file));
  });
  return new Promise((done) => server.listen(0, "127.0.0.1", () => done({ server, url: `http://127.0.0.1:${server.address().port}/` })));
}

class Page {
  constructor(socket) {
    this.socket = socket;
    this.nextId = 1;
    this.pending = new Map();
    this.errors = [];
    socket.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.id && this.pending.has(message.id)) {
        const { resolve: done, reject } = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) reject(new Error(message.error.message));
        else done(message.result);
      } else if (message.method === "Runtime.exceptionThrown") {
        this.errors.push(message.params.exceptionDetails.exception?.description ?? message.params.exceptionDetails.text);
      } else if (message.method === "Runtime.consoleAPICalled" && message.params.type === "error") {
        this.errors.push(message.params.args.map((a) => a.value ?? a.description).join(" "));
      }
    });
  }

  send(method, params = {}) {
    const id = this.nextId++;
    this.socket.send(JSON.stringify({ id, method, params }));
    return new Promise((done, reject) => this.pending.set(id, { resolve: done, reject }));
  }

  /** Runs an expression in the page and returns its value. */
  async eval(expression) {
    const result = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (result.exceptionDetails) throw new Error(`page error: ${result.exceptionDetails.exception?.description ?? result.exceptionDetails.text}`);
    return result.result.value;
  }

  /** Waits until an expression is true. */
  async waitFor(expression, timeout = 8000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) {
      if (await this.eval(`Boolean(${expression})`)) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`timed out waiting for: ${expression}\nscreen text: ${await this.text()}`);
  }

  async goto(url) {
    await this.send("Page.navigate", { url });
    await this.waitFor("document.readyState === 'complete'");
  }

  text(selector = "body") {
    return this.eval(`document.querySelector(${JSON.stringify(selector)})?.innerText ?? ""`);
  }

  click(selector) {
    return this.eval(`(() => {
      const node = document.querySelector(${JSON.stringify(selector)});
      if (!node) throw new Error("not found: ${selector.replaceAll('"', "'")}");
      node.click();
      return true;
    })()`);
  }

  /** Clicks the first element that matches the selector and contains the text. */
  clickText(selector, text) {
    return this.eval(`(() => {
      const node = [...document.querySelectorAll(${JSON.stringify(selector)})].find((n) => n.textContent.includes(${JSON.stringify(text)}));
      if (!node) throw new Error("no ${selector.replaceAll('"', "'")} with the text: " + ${JSON.stringify(text)});
      node.click();
      return true;
    })()`);
  }

  fill(name, value) {
    return this.eval(`(() => {
      const input = document.querySelector('[name=${JSON.stringify(name)}]');
      if (!input) throw new Error("no field ${name}");
      input.value = ${JSON.stringify(value)};
      input.dispatchEvent(new Event("input", { bubbles: true }));
      return true;
    })()`);
  }

  submit() {
    return this.eval(`document.querySelector("form").requestSubmit(), true`);
  }

  async screenshot(path) {
    const { data } = await this.send("Page.captureScreenshot", { format: "png" });
    const { writeFileSync } = await import("node:fs");
    writeFileSync(path, Buffer.from(data, "base64"));
  }

  close() {
    this.socket.close();
  }
}

/** Starts a headless Chrome. Returns { newPage, close }. */
export async function launchChrome(chromePath) {
  const profile = mkdtempSync(join(tmpdir(), "partiufacul-chrome-"));
  const chrome = spawn(chromePath, [
    "--headless=new",
    "--disable-gpu",
    "--no-first-run",
    "--no-default-browser-check",
    "--force-prefers-reduced-motion",
    "--remote-debugging-port=0",
    `--user-data-dir=${profile}`,
    ...(process.platform === "linux" ? ["--no-sandbox", "--disable-dev-shm-usage"] : []),
    "about:blank",
  ], {
    // Its own process group, so that close() can end the helper processes too.
    detached: process.platform !== "win32",
    stdio: ["ignore", "ignore", "pipe"],
  });
  const port = await new Promise((done, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error(`Chrome did not start:\n${output}`)), 20000);
    chrome.stderr.on("data", (chunk) => {
      output += chunk;
      const match = /DevTools listening on ws:\/\/[^:]+:(\d+)\//.exec(output);
      if (match) {
        clearTimeout(timer);
        done(match[1]);
      }
    });
  }).catch((error) => {
    stop();
    throw error;
  });
  // Node.js must not wait for Chrome or its output to end.
  chrome.unref();
  chrome.stderr.unref?.();

  function stop() {
    try {
      if (process.platform === "win32") chrome.kill();
      else process.kill(-chrome.pid, "SIGKILL");
    } catch {
      // Chrome already ended.
    }
  }

  return {
    async newPage({ width = 1100, height = 900, mobile = false } = {}) {
      const target = await (await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method: "PUT" })).json();
      const socket = new WebSocket(target.webSocketDebuggerUrl);
      await new Promise((done, reject) => {
        socket.addEventListener("open", done, { once: true });
        socket.addEventListener("error", reject, { once: true });
      });
      const page = new Page(socket);
      await page.send("Page.enable");
      await page.send("Runtime.enable");
      await page.send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile });
      return page;
    },
    async close() {
      stop();
      await new Promise((done) => {
        if (chrome.exitCode !== null || chrome.signalCode !== null) return done();
        chrome.once("exit", done);
        setTimeout(done, 3000).unref();
      });
      chrome.stderr.destroy();
      // Helper processes can write to the profile for a moment longer, so the
      // cleanup of the temporary folder is best effort.
      try {
        rmSync(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
      } catch {
        // The operating system removes temporary folders later.
      }
    },
  };
}
