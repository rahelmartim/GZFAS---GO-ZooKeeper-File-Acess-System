// Tests the dashboard in a headless Chrome, on the WebAssembly build in
// site/ (run "make web" first). It clicks through the page like a person.

import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { after, before, describe, test } from "node:test";
import { findChrome, launchChrome, serveApp } from "./browser.js";

const chromePath = findChrome();
const built = existsSync(resolve(import.meta.dirname, "..", "..", "site", "gzfas.wasm"));
const skip = !chromePath ? "Chrome is not installed" : !built ? "run make web first" : false;

describe("dashboard in the browser", { skip }, () => {
  let chrome;
  let app;
  let page;

  before(async () => {
    app = await serveApp();
    chrome = await launchChrome(chromePath);
    page = await chrome.newPage({ width: 1500, height: 1200 });
    await page.goto(app.url);
    await page.waitFor("document.querySelectorAll('.file').length === 2", 30000);
  });

  after(async () => {
    page?.close();
    await chrome?.close();
    app?.server.close();
  });

  const count = (selector) => page.eval(`document.querySelectorAll(${JSON.stringify(selector)}).length`);
  const idle = () => page.waitFor("!document.querySelector('.scenario[aria-pressed]')", 40000);
  const fileCard = (name) => `[...document.querySelectorAll('.file')].find((f) => f.querySelector('.file-name').textContent === '${name}')`;

  test("starts on the in-memory ZooKeeper with demo data", async () => {
    assert.match(await page.text("#mode"), /in-memory ZooKeeper/);
    assert.equal(await count(".client"), 3);
    assert.equal(await count(".server"), 3);
    assert.equal(await count(".server.leader"), 1);
    assert.match(await page.text("#tree"), /MAX_READ/);
  });

  test("a client asks to read and releases", async () => {
    await page.clickText(".client strong", "bia");
    await page.waitFor("document.querySelector('#acting').textContent === 'acting as bia'");
    await page.eval(`${fileCard("access.csv")}.querySelector('.file-actions .btn').click(), true`);
    await page.waitFor(`${fileCard("access.csv")}.classList.contains('status-read')`);
    assert.match(await page.eval(`${fileCard("access.csv")}.querySelector('.queue').textContent`), /read-\d{10}.*bia.*held/);
    await page.eval(`[...${fileCard("access.csv")}.querySelectorAll('.file-actions .btn')].find((b) => b.textContent === 'Release').click(), true`);
    await page.waitFor(`${fileCard("access.csv")}.classList.contains('status-free')`);
  });

  test("a writer edits and saves a new version", async () => {
    await page.clickText(".client strong", "caio");
    await page.waitFor("document.querySelector('#acting').textContent === 'acting as caio'");
    const versions = await page.eval(`${fileCard("access.csv")}.querySelectorAll('.versions li').length`);
    await page.eval(`[...${fileCard("access.csv")}.querySelectorAll('.file-actions .btn')].find((b) => b.textContent === 'Ask to write').click(), true`);
    await page.waitFor(`${fileCard("access.csv")}.classList.contains('status-write')`);
    await page.eval(`[...${fileCard("access.csv")}.querySelectorAll('.file-actions .btn')].find((b) => b.textContent === 'Edit').click(), true`);
    await page.waitFor("document.querySelector('#dialog').open");
    await page.eval(`(() => {
      document.querySelector('#dialog-content').value = 'date,reader,file\\n2026-11-12,caio,access.csv\\n';
      document.querySelector('#dialog-note').value = 'new row';
      document.querySelector('#dialog-save').click();
      return true;
    })()`);
    await page.waitFor(`${fileCard("access.csv")}.querySelectorAll('.versions li').length === ${versions + 1}`);
    assert.match(await page.eval(`${fileCard("access.csv")}.querySelector('.preview').textContent`), /caio,access\.csv/);
    await page.eval(`[...${fileCard("access.csv")}.querySelectorAll('.file-actions .btn')].find((b) => b.textContent === 'Release').click(), true`);
    await page.waitFor(`${fileCard("access.csv")}.classList.contains('status-free')`);
  });

  test("the writer waits scenario shows the watch and ends free", async () => {
    await page.clickText(".scenario", "The writer waits");
    await page.waitFor(`${fileCard("notes.md")}.querySelector('.qnode.waiting .watch')`, 15000);
    assert.match(await page.eval(`${fileCard("notes.md")}.querySelector('.qnode.waiting .watch').textContent`), /watches read-/);
    await idle();
    assert.match(await page.text("#narration"), /file is free/);
    assert.match(await page.text("#ledger"), /WATCHER::deleted/);
    assert.match(await page.text("#ledger"), /create -e -s \/GZFAS\/data\/notes\.md\/locks\/write-/);
  });

  test("a client that crashes loses its ephemeral znodes", async () => {
    await page.clickText(".scenario", "A client crashes");
    await idle();
    assert.equal(await page.eval("[...document.querySelectorAll('.client strong')].some((s) => s.textContent === 'ana')"), false);
    assert.doesNotMatch(await page.text("#tree"), /client ana/);
    assert.match(await page.text("#narration"), /reads now/);
  });

  test("the leader falls and another one leads", async () => {
    const before = await page.text(".server.leader strong");
    await page.clickText(".scenario", "The leader falls");
    await idle();
    const after = await page.text(".server.leader strong");
    assert.notEqual(after, before);
    assert.equal(await count(".server"), 3);
  });

  test("MAX_READ makes the third reader wait on the folder", async () => {
    await page.clickText(".scenario", "MAX_READ");
    await page.waitFor(`${fileCard("notes.md")}.textContent.includes('watches locks/')`, 15000);
    await idle();
    // The scenario sets MAX_READ back to 3 at the end.
    assert.equal(await page.eval("document.querySelectorAll('#conf output')[1].textContent"), "3");
  });

  test("restoring a version adds to the history", async () => {
    const before = await page.eval(`${fileCard("notes.md")}.querySelectorAll('.versions li').length`);
    await page.clickText(".scenario", "Restore a version");
    await idle();
    assert.equal(await page.eval(`${fileCard("notes.md")}.querySelectorAll('.versions li').length`), before + 2);
    assert.match(await page.eval(`${fileCard("notes.md")}.querySelector('.versions').textContent`), /rollback to v-0000000000/);
  });

  test("errors appear as messages", async () => {
    await page.eval("document.querySelectorAll('.toast').forEach((t) => t.remove()), true");
    await page.eval(`(() => {
      const input = document.querySelector('#client-form input');
      input.value = 'bad name!';
      document.querySelector('#client-form').requestSubmit();
      return true;
    })()`);
    await page.waitFor("document.querySelector('.toast.error')");
    assert.match(await page.text(".toast.error"), /names use/);
  });

  test("reset starts a new tree", async () => {
    await page.clickText(".scenario", "Reset");
    await page.waitFor("document.querySelector('#narration').textContent.includes('new, empty')", 15000);
    assert.equal(await count(".client"), 3);
    assert.equal(await count(".file"), 2);
  });

  test("the page had no JavaScript errors", () => {
    assert.deepEqual(page.errors, []);
  });
});
