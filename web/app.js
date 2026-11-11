/* GZFAS dashboard. The same page works with two backends:
   - the HTTP API of gzfas-server, on a real ZooKeeper
   - GZFAS compiled to WebAssembly, on an in-memory ZooKeeper (GitHub Pages) */

const $ = (selector) => document.querySelector(selector);

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? "" : String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

/* Backends */

async function httpBackend() {
  const hello = await fetch("api/hello", { cache: "no-store" });
  if (!hello.ok || !(hello.headers.get("Content-Type") ?? "").includes("json")) throw new Error("no API");
  const info = await hello.json();
  return {
    mode: `ZooKeeper · ${info.result.mode}`,
    canReset: false,
    async do(action, args = {}) {
      const response = await fetch(`api/do/${encodeURIComponent(action)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(args),
      });
      return unwrap(await response.json());
    },
    onCall(fn) {
      const source = new EventSource("api/events");
      source.onmessage = (event) => fn(JSON.parse(event.data));
    },
  };
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const script = el("script", { src });
    script.onload = resolve;
    script.onerror = () => reject(new Error(`cannot load ${src}`));
    document.head.append(script);
  });
}

async function wasmBackend() {
  await loadScript("wasm_exec.js");
  const go = new window.Go();
  const ready = new Promise((resolve) => window.addEventListener("gzfas-ready", resolve, { once: true }));
  const result = await WebAssembly.instantiateStreaming(fetch("gzfas.wasm"), go.importObject);
  go.run(result.instance);
  await ready;
  return {
    mode: "in-memory ZooKeeper · in your browser",
    canReset: true,
    async do(action, args = {}) {
      return unwrap(JSON.parse(await window.gzfas.do(action, JSON.stringify(args))));
    },
    onCall(fn) {
      window.gzfas.onCall = (json) => fn(JSON.parse(json));
    },
    async reset() {
      await window.gzfas.reset();
    },
  };
}

function unwrap(reply) {
  if (!reply.ok) throw new Error(reply.error);
  return reply.result;
}

/* State */

const state = {
  backend: null,
  snapshot: null,
  calls: [],
  active: null,
  known: new Set(), // znode paths seen in the last render
  busy: false,
};

const toArgs = (args) => Object.fromEntries(Object.entries(args).map(([k, v]) => [k, String(v)]));

async function act(action, args = {}, { quiet = false } = {}) {
  try {
    const result = await state.backend.do(action, toArgs(args));
    await refresh();
    return result;
  } catch (error) {
    if (!quiet) toast(error.message, "error");
    await refresh();
    throw error;
  }
}

let refreshTimer = 0;
function scheduleRefresh() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(refresh, 90);
}

async function refresh() {
  try {
    const snap = await state.backend.do("snapshot");
    for (const key of ["servers", "clients", "files"]) snap[key] ??= [];
    state.snapshot = snap;
    render();
  } catch (error) {
    console.error(error);
  }
}

function toast(text, kind = "info") {
  const node = el("div", { class: `toast ${kind}`, role: "status" }, text);
  $("#toasts").append(node);
  setTimeout(() => node.remove(), 4200);
}

/* Rendering */

function render() {
  const snap = state.snapshot;
  if (!snap) return;
  if (state.active && !snap.clients?.some((c) => c.name === state.active)) state.active = null;
  renderServers(snap);
  renderConf(snap);
  renderClients(snap);
  renderFiles(snap);
  renderTree(snap);
  $("#acting").textContent = state.active ? `acting as ${state.active}` : "choose a client on the left";
}

function renderServers(snap) {
  const list = $("#servers");
  list.replaceChildren();
  for (const s of snap.servers ?? []) {
    list.append(
      el("li", { class: `server${s.leader ? " leader" : ""}` },
        el("span", { class: "role" }, s.leader ? "LEADER" : "FOLLOWER"),
        el("span", { class: "who" },
          el("strong", {}, s.id),
          el("small", {}, `${s.node}${s.leader ? " · leads" : s.watching ? ` · watches ${s.watching}` : ""}`)),
        el("button", { class: "btn btn-quiet", title: "End its session, as a crash would", onclick: () => act("stopServer", { server: s.id }) }, "Stop")),
    );
  }
  if (!snap.servers?.length) list.append(el("li", { class: "hint" }, "No server runs. Clients cannot lock or write."));
}

function renderConf(snap) {
  const box = $("#conf");
  box.replaceChildren();
  const rows = [
    ["MAX_CONN", "clients at once"],
    ["MAX_READ", "readers of one file"],
  ];
  for (const [key, text] of rows) {
    const value = snap.conf?.[key] ?? "?";
    box.append(
      el("div", { class: "conf-row" },
        el("span", {}, el("code", {}, key), el("span", { class: "hint" }, text)),
        el("span", { class: "stepper" },
          el("button", { class: "btn", "aria-label": `Lower ${key}`, onclick: () => act("setConf", { key, value: Number(value) - 1 }) }, "−"),
          el("output", {}, value),
          el("button", { class: "btn", "aria-label": `Raise ${key}`, onclick: () => act("setConf", { key, value: Number(value) + 1 }) }, "+"))),
    );
  }
}

function renderClients(snap) {
  const list = $("#clients");
  list.replaceChildren();
  for (const c of snap.clients ?? []) {
    const pick = () => {
      state.active = c.name;
      render();
    };
    list.append(
      el("li", {
          class: `client${c.name === state.active ? " active" : ""}`,
          tabindex: 0, role: "button", "aria-pressed": String(c.name === state.active),
          onclick: pick, onkeydown: (e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), pick()),
        },
        el("div", { class: "client-top" },
          el("span", { class: "who" }, el("strong", {}, c.name), el("small", {}, `session ${c.session}`)),
          el("button", {
            class: "btn btn-quiet", title: "End the session: ZooKeeper deletes its ephemeral znodes",
            onclick: (e) => {
              e.stopPropagation();
              act("disconnect", { client: c.name });
            },
          }, "Disconnect")),
        el("p", { class: "client-status" }, c.status),
        c.locks.length
          ? el("div", { class: "tags" }, ...c.locks.map((l) => el("span", { class: `tag ${l.state} ${l.mode}`, title: l.error ?? "" }, `${l.mode} ${l.file} · ${l.state}`)))
          : null),
    );
  }
  if (!snap.clients?.length) list.append(el("li", { class: "hint" }, "No client is connected."));
}

function lockOf(file) {
  const client = state.snapshot?.clients?.find((c) => c.name === state.active);
  return client?.locks.find((l) => l.file === file) ?? null;
}

function renderFiles(snap) {
  const box = $("#files");
  box.replaceChildren();
  for (const f of snap.files ?? []) {
    const mine = lockOf(f.name);
    const readers = f.queue.filter((q) => q.state === "held" && q.mode === "read").length;
    const badge = f.status === "write" ? "write lock" : f.status === "read" ? `read lock ×${readers}` : "free";
    const actions = [];
    const can = Boolean(state.active);
    if (!mine) {
      actions.push(
        el("button", { class: "btn", disabled: !can, onclick: () => act("lock", { client: state.active, file: f.name, mode: "read" }) }, "Ask to read"),
        el("button", { class: "btn btn-accent", disabled: !can, onclick: () => act("lock", { client: state.active, file: f.name, mode: "write" }) }, "Ask to write"),
      );
    } else if (mine.state === "waiting") {
      actions.push(el("button", { class: "btn", onclick: () => act("release", { client: state.active, file: f.name }) }, "Cancel the request"));
    } else if (mine.state === "held") {
      actions.push(el("button", { class: "btn", onclick: () => openFile(f, mine.mode) }, mine.mode === "write" ? "Edit" : "Read"));
      if (mine.mode === "write") {
        actions.push(el("button", {
          class: "btn btn-quiet", onclick: () => {
            if (confirm(`Delete ${f.name} and its whole history?`)) act("delete", { client: state.active, file: f.name });
          },
        }, "Delete"));
      }
      actions.push(el("button", { class: "btn btn-accent", onclick: () => act("release", { client: state.active, file: f.name }) }, "Release"));
    }

    const queue = el("ol", { class: "queue" },
      ...f.queue.map((q) =>
        el("li", { class: `qnode ${q.state} ${q.mode}`, title: `${q.mode} lock of ${q.owner} · ${q.state}` },
          el("code", {}, q.node),
          el("small", {}, `${q.owner?.replace(/^client /, "") ?? "?"} · ${q.state}`),
          q.watching ? el("div", { class: "watch" }, `watches ${q.watching}`) : null)));

    const versions = el("ol", { class: "versions" },
      ...[...f.versions].reverse().map((v) =>
        el("li", { class: v.name === f.meta.current ? "current" : "" },
          el("code", { title: v.name }, `v${Number(v.name.slice(2))}`),
          el("span", {}, `${v.author}`, v.note ? el("em", {}, ` · ${v.note}`) : null),
          mine?.state === "held" && mine.mode === "write" && v.name !== f.meta.current
            ? el("button", { class: "btn btn-quiet", onclick: () => act("rollback", { client: state.active, file: f.name, version: v.name }) }, "restore")
            : el("span", { class: "hint" }, `${v.size} B`))));

    box.append(
      el("article", { class: `file status-${f.status}` },
        el("div", { class: "file-head" }, el("h3", { class: "file-name" }, f.name), el("span", { class: "badge" }, badge)),
        el("p", { class: "file-meta" }, `${f.meta.current || "no version"} · ${f.meta.size} B · sha256 ${f.meta.sha256?.slice(0, 10) ?? ""}…`),
        el("pre", { class: "preview" }, f.content || "(empty)"),
        el("p", { class: "queue-title" }, "Lock queue · ", el("code", {}, `/GZFAS/data/${f.name}/locks`)),
        queue,
        el("p", { class: "versions-title" }, "Versions"),
        versions,
        el("div", { class: "file-actions" }, ...actions)),
    );
  }
  if (!snap.files?.length) box.append(el("p", { class: "hint" }, "No file yet. Choose a client and create one."));
}

function renderTree(snap) {
  const seen = new Set();
  const draw = (node) => {
    seen.add(node.path);
    const born = state.known.size > 0 && !state.known.has(node.path);
    const line = el("div", { class: `znode${node.ephemeral ? " ephemeral" : ""}${born ? " born" : ""}`, role: "treeitem" },
      el("span", { class: "name" }, node.name),
      node.ephemeral ? el("span", { class: "eph", title: "ephemeral" }, "●") : null,
      node.data ? el("span", { class: "data", title: node.data }, node.data) : null,
      node.ephemeral ? el("span", { class: "owner" }, node.owner) : null,
      node.version ? el("span", { class: "ver" }, `v${node.version}`) : null);
    const item = el("li", {}, line);
    if (node.children.length) item.append(el("ul", { role: "group" }, ...node.children.map(draw)));
    return item;
  };
  const scroll = $("#tree").scrollTop;
  $("#tree").replaceChildren(el("ul", {}, draw(snap.tree)));
  $("#tree").scrollTop = scroll;
  state.known = seen;
}

/* Ledger */

const READS = new Set(["get", "exists", "children"]);

function addCall(call) {
  state.calls.push(call);
  if (state.calls.length > 300) state.calls.shift();
  if (call.actor !== "dashboard" && !(READS.has(call.kind) && $("#quiet").checked)) {
    const ledger = $("#ledger");
    ledger.prepend(
      el("li", { class: `k-${call.kind}${call.error ? " error" : ""}` },
        el("span", { class: "actor", title: `${call.actor} · session ${call.session}` }, call.actor.replace(/^(client|server) /, "")),
        el("span", {}, el("span", { class: "cmd" }, call.command), call.result ? el("span", { class: "result" }, ` → ${call.result}`) : null)),
    );
    while (ledger.children.length > 200) ledger.lastElementChild.remove();
  }
  scheduleRefresh();
}

/* File dialog */

let dialogFile = null;

async function openFile(file, mode) {
  try {
    const { content, meta } = await state.backend.do("read", toArgs({ client: state.active, file: file.name }));
    dialogFile = { name: file.name, mode };
    $("#dialog-title").textContent = file.name;
    $("#dialog-meta").textContent = `${meta.current} · ${meta.size} B · last written by ${meta.updatedBy}`;
    $("#dialog-content").value = content;
    $("#dialog-content").readOnly = mode !== "write";
    $("#dialog-note").hidden = mode !== "write";
    $("#dialog-note").value = "";
    $("#dialog-save").hidden = mode !== "write";
    $("#dialog").showModal();
  } catch (error) {
    toast(error.message, "error");
  }
}

$("#dialog-form").addEventListener("submit", async (event) => {
  if (event.submitter?.value !== "save" || !dialogFile) return;
  await act("write", { client: state.active, file: dialogFile.name, content: $("#dialog-content").value, note: $("#dialog-note").value }).catch(() => {});
});

/* Forms */

function formHandler(selector, run) {
  $(selector).addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const values = Object.fromEntries(new FormData(form));
    try {
      await run(values);
      form.reset();
    } catch {
      // act() already showed the error.
    }
  });
}

formHandler("#client-form", async ({ client }) => {
  await act("connect", { client: client.trim() });
  state.active = client.trim();
  render();
});
formHandler("#server-form", ({ server }) => act("startServer", { server: server.trim() }));
formHandler("#file-form", ({ file, content }) => {
  if (!state.active) {
    toast("Choose a client first: files are created by a client.", "error");
    return Promise.reject(new Error("no client"));
  }
  return act("create", { client: state.active, file: file.trim(), content });
});

$("#quiet").addEventListener("change", () => {
  $("#ledger").replaceChildren();
  const calls = state.calls.splice(0);
  calls.forEach(addCall);
});

/* Scenarios */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function narrate(...parts) {
  $("#narration").replaceChildren(...parts.map((p) => (Array.isArray(p) ? el("strong", {}, p[0]) : p)));
}

// waitHeld waits until a lock request of a client is held. A request
// returns at once, and the lock comes when ZooKeeper says so.
async function waitHeld(client, file, timeout = 10000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    await refresh();
    const lock = state.snapshot.clients.find((c) => c.name === client)?.locks.find((l) => l.file === file);
    if (lock?.state === "held") return;
    await sleep(60);
  }
  throw new Error(`${client} did not get the lock of ${file}`);
}

async function lockAndWait(client, file, mode) {
  await act("lock", { client, file, mode });
  await waitHeld(client, file);
}

async function ensureClient(name) {
  if (!state.snapshot.clients.some((c) => c.name === name)) await act("connect", { client: name }, { quiet: true });
}

async function releaseAll(names) {
  for (const name of names) {
    const c = state.snapshot.clients.find((x) => x.name === name);
    for (const l of c?.locks ?? []) await act("release", { client: name, file: l.file }, { quiet: true }).catch(() => {});
  }
}

async function ensureFile(name, content) {
  if (!state.snapshot.files.some((f) => f.name === name)) {
    await ensureClient("ana");
    await act("create", { client: "ana", file: name, content }, { quiet: true });
  }
}

async function ensureServers() {
  for (const id of ["s1", "s2", "s3"]) {
    if (!state.snapshot.servers.some((s) => s.id === id)) await act("startServer", { server: id }, { quiet: true }).catch(() => {});
  }
}

async function prepare(clients) {
  await refresh();
  await ensureServers();
  if ((state.snapshot.conf.MAX_READ ?? 3) < 3) await act("setConf", { key: "MAX_READ", value: 3 }, { quiet: true });
  for (const c of clients) await ensureClient(c);
  // Release every lock, so that an earlier scenario cannot block this one.
  await releaseAll(state.snapshot.clients.map((c) => c.name));
  await ensureFile("notes.md", "Locks keep the order.\n");
}

const step = 1400;

const SCENARIOS = [
  {
    name: "Readers share",
    async run() {
      await prepare(["ana", "bia"]);
      narrate(["ana"], " asks to read. GZFAS creates an ephemeral sequential znode under ", ["locks/"], ".");
      await act("lock", { client: "ana", file: "notes.md", mode: "read" });
      await sleep(step);
      narrate(["bia"], " asks to read too. No write znode comes before hers, so she reads at the same time.");
      await act("lock", { client: "bia", file: "notes.md", mode: "read" });
      await sleep(step);
      narrate("Two read znodes hold the file. Look at the tree: both have a ", ["●"], ": they live only as long as their sessions.");
    },
  },
  {
    name: "The writer waits",
    async run() {
      await prepare(["ana", "bia", "caio"]);
      await lockAndWait("ana", "notes.md", "read");
      await lockAndWait("bia", "notes.md", "read");
      narrate(["ana"], " and ", ["bia"], " read the file.");
      await sleep(step);
      narrate(["caio"], " asks to write. A writer must be first in the queue, so he waits and watches only the znode right before his.");
      await act("lock", { client: "caio", file: "notes.md", mode: "write" });
      await sleep(step * 1.4);
      narrate(["bia"], " finishes. ZooKeeper fires the watch of caio, who checks again: ana still reads, so he watches her znode now.");
      await act("release", { client: "bia", file: "notes.md" });
      await sleep(step * 1.4);
      narrate(["ana"], " finishes. Caio is first now: he holds the write lock alone.");
      await act("release", { client: "ana", file: "notes.md" });
      await waitHeld("caio", "notes.md");
      await sleep(step);
      narrate(["caio"], " writes a new version: a sequential znode under ", ["versions/"], ", then ", ["set -v"], " on the file znode.");
      await act("write", { client: "caio", file: "notes.md", content: "Locks keep the order.\nWriters wait for their turn.\n", note: "second line" });
      await sleep(step);
      await act("release", { client: "caio", file: "notes.md" });
      narrate("Caio releases. The file is free again.");
    },
  },
  {
    name: "A client crashes",
    async run() {
      await prepare(["ana", "bia"]);
      narrate(["ana"], " takes the write lock.");
      await lockAndWait("ana", "notes.md", "write");
      await sleep(step);
      narrate(["bia"], " asks to read, and waits for ana.");
      await act("lock", { client: "bia", file: "notes.md", mode: "read" });
      await sleep(step * 1.3);
      narrate(["ana"], " crashes. Her session ends, and ZooKeeper deletes her ephemeral znodes: her client znode and her lock.");
      await act("disconnect", { client: "ana" });
      await sleep(step);
      narrate("The watch of ", ["bia"], " fires. She reads now. No lock stays behind a dead client.");
    },
  },
  {
    name: "The leader falls",
    async run() {
      await prepare([]);
      const leader = state.snapshot.servers.find((s) => s.leader);
      narrate(["s1"], ", ", ["s2"], ", and ", ["s3"], " each own a znode under ", ["servers/"], ". The lowest one leads, and each other server watches the one before it.");
      await sleep(step * 1.3);
      if (leader) {
        narrate("The leader ", [leader.id], " stops. Its session ends, ", ["/GZFAS/leader"], " goes, and the next server is notified.");
        await act("stopServer", { server: leader.id });
        await sleep(step * 1.3);
        const next = state.snapshot.servers.find((s) => s.leader);
        narrate(next ? [next.id] : "No server", " leads now. The stopped server starts again and joins the end of the line.");
        await sleep(step);
        await act("startServer", { server: leader.id });
      }
    },
  },
  {
    name: "MAX_READ",
    async run() {
      await prepare(["ana", "bia", "caio"]);
      narrate("MAX_READ becomes ", ["2"], ": at most two readers of one file at the same time.");
      await act("setConf", { key: "MAX_READ", value: 2 });
      await sleep(step);
      await lockAndWait("ana", "notes.md", "read");
      await lockAndWait("bia", "notes.md", "read");
      await act("lock", { client: "caio", file: "notes.md", mode: "read" });
      narrate(["caio"], " is the third reader, so he waits. He watches the whole ", ["locks/"], " folder for a free place.");
      await sleep(step * 1.4);
      await act("release", { client: "ana", file: "notes.md" });
      narrate(["ana"], " leaves, and ", ["caio"], " reads.");
      await waitHeld("caio", "notes.md");
      await sleep(step);
      await act("setConf", { key: "MAX_READ", value: 3 });
      await releaseAll(["bia", "caio"]);
    },
  },
  {
    name: "Restore a version",
    async run() {
      await prepare(["bia"]);
      await lockAndWait("bia", "notes.md", "write");
      narrate(["bia"], " holds the write lock, and writes a bad version.");
      await act("write", { client: "bia", file: "notes.md", content: "(deleted by mistake)\n", note: "oops" });
      await sleep(step);
      const first = state.snapshot.files.find((f) => f.name === "notes.md").versions[0];
      narrate("She restores ", [first.name], ". The old content becomes a new version: history only grows.");
      await act("rollback", { client: "bia", file: "notes.md", version: first.name });
      await sleep(step);
      await act("release", { client: "bia", file: "notes.md" });
    },
  },
];

function setupScenarios() {
  const box = $("#scenarios");
  for (const scenario of SCENARIOS) {
    const button = el("button", { class: "scenario", type: "button" }, scenario.name);
    button.addEventListener("click", async () => {
      if (state.busy) return;
      state.busy = true;
      box.querySelectorAll("button").forEach((b) => (b.disabled = b !== button));
      button.setAttribute("aria-pressed", "true");
      try {
        await scenario.run();
      } catch (error) {
        narrate("The scenario stopped: ", error.message);
      } finally {
        state.busy = false;
        button.removeAttribute("aria-pressed");
        box.querySelectorAll("button").forEach((b) => (b.disabled = false));
      }
    });
    box.append(button);
  }
  if (state.backend.canReset) {
    box.append(el("button", {
      class: "scenario reset", type: "button",
      onclick: async () => {
        await state.backend.reset();
        $("#ledger").replaceChildren();
        state.calls = [];
        state.known = new Set();
        await seed();
        narrate("A new, empty ZooKeeper tree.");
      },
    }, "Reset"));
  }
}

/* Start */

async function seed() {
  await refresh();
  const snap = state.snapshot;
  if (snap.clients.length || snap.files.length) return;
  try {
    for (const name of ["ana", "bia", "caio"]) await act("connect", { client: name });
    await act("create", { client: "ana", file: "notes.md", content: "Locks keep the order.\n" });
    await act("create", { client: "bia", file: "access.csv", content: "date,reader,file\n2026-11-11,ana,notes.md\n" });
  } catch {
    narrate("The demo data could not be created. The message in the corner says why.");
  }
  state.active = "ana";
  render();
}

async function start() {
  try {
    state.backend = await httpBackend().catch(() => wasmBackend());
  } catch (error) {
    $("#mode").textContent = `Could not start: ${error.message}`;
    return;
  }
  $("#mode").textContent = state.backend.mode;
  state.backend.onCall(addCall);
  setupScenarios();
  await seed();
  await refresh();
}

start();
