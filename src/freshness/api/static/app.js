"use strict";

const SAMPLE_TITLE = "Travel and expense policy";
const SAMPLE = [
  "Employees travelling on company business should book flights through the approved " +
    "travel portal at least fourteen days in advance. Economy class is the default for " +
    "flights shorter than six hours.",
  "Hotel stays are reimbursed up to a nightly limit of 200 dollars in most cities. Stays " +
    "in high-cost cities such as New York or London may be approved up to 300 dollars per " +
    "night by a manager.",
  "Meals during travel are covered by a daily allowance of 75 dollars. Alcohol is not " +
    "reimbursable, and receipts are required for any single meal above 25 dollars.",
  "Expense reports must be submitted within thirty days of returning from a trip. Late " +
    "reports require approval from the finance team before payment.",
].join("\n\n");
const MILEAGE =
  "Employees who drive their own car on company business are reimbursed at 67 cents per " +
  "mile. Commuting between home and the usual office is not covered.";

const STORAGE_KEY = "freshness-demo-document";
const POLL_MS = 40;
const GIVE_UP_MS = 60000;
const SCALES = [100, 200, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000, 20000, 60000];

const $ = (id) => document.getElementById(id);
const els = {
  title: $("title"),
  content: $("content"),
  save: $("save"),
  del: $("delete"),
  presets: $("presets"),
  editorError: $("editor-error"),
  docVersion: $("doc-version"),
  indexVersion: $("index-version"),
  chunks: $("chunks"),
  search: $("search"),
  query: $("query"),
  results: $("results"),
  timeline: $("timeline"),
  axis: $("axis"),
  run: $("run"),
  summary: $("track-summary"),
  history: $("history"),
  historySection: $("history-section"),
};
const marks = {
  saved: els.timeline.querySelector('[data-mark="saved"]'),
  published: els.timeline.querySelector('[data-mark="published"]'),
  indexed: els.timeline.querySelector('[data-mark="indexed"]'),
};

let docId = null;
let busy = false;

/* Storage is a convenience: the page works without it. */
function remember(id) {
  try {
    if (id) localStorage.setItem(STORAGE_KEY, id);
    else localStorage.removeItem(STORAGE_KEY);
  } catch {}
}

function remembered() {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

async function api(method, path, body) {
  const response = await fetch(path, {
    method,
    headers: body ? { "content-type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const data = await response.json();
      if (typeof data.detail === "string") detail = data.detail;
      else if (Array.isArray(data.detail)) detail = data.detail.map((d) => d.msg).join("; ");
    } catch {}
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const ms = (from, to) => new Date(to) - new Date(from);
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

function formatMs(value) {
  if (value < 1000) return `${Math.round(value)} ms`;
  return `${(value / 1000).toFixed(value < 10000 ? 2 : 1)} s`;
}

/* Timeline */

function scaleFor(value) {
  return SCALES.find((s) => s >= value * 1.15) ?? SCALES[SCALES.length - 1];
}

function drawAxis(scale) {
  els.axis.replaceChildren();
  for (let i = 0; i <= 4; i++) {
    const tick = document.createElement("span");
    tick.className = "tick";
    tick.style.left = `${i * 25}%`;
    tick.textContent = formatMs((scale * i) / 4);
    els.axis.append(tick);
  }
}

function place(mark, at, scale, label, value) {
  mark.style.left = `${(Math.min(at, scale) / scale) * 100}%`;
  mark.classList.add("on");
  const text = mark.querySelector(".mark-label");
  text.replaceChildren();
  const strong = document.createElement("b");
  strong.textContent = value;
  text.append(strong, label);
}

function hide(mark) {
  mark.classList.remove("on");
}

function resetTimeline() {
  els.timeline.dataset.state = "idle";
  els.run.style.width = "0";
  drawAxis(250);
  Object.values(marks).forEach(hide);
}

/* While waiting, the bar grows on the browser's clock; the final drawing uses
   only database timestamps, so the numbers shown are the server's. */
function animateWaiting() {
  const started = performance.now();
  let frame = 0;
  let scale = 0;
  els.timeline.dataset.state = "waiting";
  Object.values(marks).forEach(hide);
  const tick = () => {
    const elapsed = performance.now() - started;
    const next = scaleFor(Math.max(elapsed, 100));
    if (next !== scale) {
      scale = next;
      drawAxis(scale);
    }
    place(marks.saved, 0, scale, "Saved", "0 ms");
    els.run.style.width = `${(elapsed / scale) * 100}%`;
    frame = requestAnimationFrame(tick);
  };
  tick();
  return () => cancelAnimationFrame(frame);
}

function drawTimeline(status, deleted) {
  const published = status.published_at ? ms(status.updated_at, status.published_at) : null;
  const indexed = ms(status.updated_at, status.indexed_at);
  const scale = scaleFor(Math.max(indexed, 100));
  els.timeline.dataset.state = "done";
  drawAxis(scale);
  place(marks.saved, 0, scale, deleted ? "Deleted" : "Saved", "0 ms");
  if (published !== null) {
    place(marks.published, published, scale, "Sent to Kafka", formatMs(published));
    marks.published.classList.add("lift");
  } else {
    hide(marks.published);
  }
  place(marks.indexed, indexed, scale, deleted ? "Removed from index" : "Indexed", formatMs(indexed));
  els.run.style.width = `${(Math.min(indexed, scale) / scale) * 100}%`;
  return { published, indexed };
}

/* Index view and search */

function drawChunks(status) {
  els.chunks.replaceChildren();
  if (!status || status.chunks.length === 0) {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = status?.deleted
      ? "The document was deleted, and every one of its chunks is gone from the index."
      : "Nothing is indexed yet. Create the document and its chunks appear here.";
    els.chunks.append(empty);
    els.indexVersion.textContent = "";
    return;
  }
  const latest = status.indexed_version;
  els.indexVersion.textContent = `Version ${latest}, ${plural(status.chunks.length, "chunk")}`;
  for (const chunk of status.chunks) {
    const item = document.createElement("li");
    const fresh = chunk.embedded_at_version === latest;
    item.className = fresh ? "chunk new" : "chunk";
    const pos = document.createElement("span");
    pos.className = "chunk-pos";
    pos.textContent = chunk.position + 1;
    const text = document.createElement("p");
    text.className = "chunk-text";
    text.textContent = chunk.content;
    const meta = document.createElement("span");
    meta.className = "chunk-meta";
    meta.textContent = fresh
      ? latest === 1
        ? "Embedded when the document was created"
        : `Embedded for version ${latest}`
      : `Kept from version ${chunk.embedded_at_version}, not embedded again`;
    item.append(pos, text, meta);
    els.chunks.append(item);
  }
}

function showResultsMessage(message) {
  const item = document.createElement("li");
  item.className = "empty";
  item.textContent = message;
  els.results.replaceChildren(item);
}

async function runSearch() {
  const query = els.query.value.trim();
  if (!docId) return showResultsMessage("Create the document first, then search it.");
  if (!query) return showResultsMessage("Type a question to search the document.");
  try {
    const response = await api("POST", "/search", { query, top_k: 3, document_id: docId });
    // Chunks that share no words with the query score zero; they aren't answers.
    const results = response.results.filter((hit) => hit.score > 0.01);
    if (results.length === 0) {
      return showResultsMessage("No chunk in the index shares words with that question. Try different wording.");
    }
    els.results.replaceChildren(
      ...results.map((hit) => {
        const item = document.createElement("li");
        item.className = "result";
        const meta = document.createElement("span");
        meta.className = "result-meta";
        const version = document.createElement("b");
        version.textContent = `Version ${hit.document_version}`;
        meta.append(version, `, chunk ${hit.position + 1}, similarity ${hit.score.toFixed(2)}`);
        const text = document.createElement("p");
        text.textContent = hit.content;
        item.append(meta, text);
        return item;
      }),
    );
  } catch (error) {
    showResultsMessage(`Search failed: ${error.message}.`);
  }
}

/* History */

function addHistory(version, change, timings, embedded, kept) {
  els.historySection.hidden = false;
  const row = document.createElement("tr");
  const cells = [
    String(version),
    change,
    timings.published === null ? "not recorded" : formatMs(timings.published),
    formatMs(timings.indexed),
    embedded === null ? "" : String(embedded),
    kept === null ? "" : String(kept),
  ];
  cells.forEach((value, i) => {
    const cell = document.createElement("td");
    cell.textContent = value;
    if (i >= 2) cell.className = "num";
    row.append(cell);
  });
  els.history.prepend(row);
}

/* Pipeline round trip */

async function waitForIndex(id, version) {
  const deadline = performance.now() + GIVE_UP_MS;
  while (performance.now() < deadline) {
    const status = await api("GET", `/documents/${id}/index`);
    // A newer edit may already have overtaken this one; the index only keeps the latest.
    if (status.indexed_version !== null && status.indexed_version >= version) {
      // published_at may land a moment after the indexer commits under load.
      if (!status.published_at) {
        await sleep(POLL_MS);
        return api("GET", `/documents/${id}/index`);
      }
      return status;
    }
    await sleep(POLL_MS);
  }
  throw new Error(
    "the index has not caught up after a minute. The indexer may be retrying; reload to check again",
  );
}

function setBusy(value) {
  busy = value;
  els.save.disabled = value;
  els.del.disabled = value;
  els.presets.querySelectorAll("button").forEach((b) => (b.disabled = value));
}

function showError(message) {
  els.editorError.hidden = !message;
  els.editorError.textContent = message ? `${message}.` : "";
}

function describeEdit(before, after) {
  if (before === after) return "Saved with no changes";
  return "Edited by hand";
}

let lastSaved = { title: "", content: "" };

async function save(change) {
  if (busy) return;
  const title = els.title.value.trim();
  const content = els.content.value;
  if (!title) return showError("Give the document a title");
  showError("");
  setBusy(true);
  els.summary.textContent = "Waiting for the pipeline…";
  const stop = animateWaiting();
  try {
    const creating = !docId;
    const doc = creating
      ? await api("POST", "/documents", { title, content })
      : await api("PUT", `/documents/${docId}`, { title, content });
    docId = doc.id;
    remember(docId);
    const label =
      change ??
      (creating
        ? "Created"
        : describeEdit(lastSaved.title + lastSaved.content, title + content));
    lastSaved = { title, content };
    els.docVersion.textContent = `Version ${doc.version} saved`;

    const status = await waitForIndex(docId, doc.version);
    stop();
    const timings = drawTimeline(status, false);
    const embedded = status.chunks.filter(
      (c) => c.embedded_at_version === status.indexed_version,
    ).length;
    const kept = status.chunks.length - embedded;
    els.summary.innerHTML = "";
    const strong = document.createElement("strong");
    strong.textContent = formatMs(timings.indexed);
    els.summary.append(
      `Version ${status.indexed_version} reached the search index `,
      strong,
      ` after it was saved. ${plural(embedded, "chunk")} embedded, ${kept} kept as they were.`,
    );
    addHistory(status.indexed_version, label, timings, embedded, kept);
    drawChunks(status);
    showEditing();
    await runSearch();
  } catch (error) {
    stop();
    resetTimeline();
    els.summary.textContent = "The last change did not finish.";
    if (error.status === 404) {
      forget();
      showError("That document no longer exists. Create it again to continue");
    } else {
      showError(`Could not save: ${error.message}`);
    }
  } finally {
    setBusy(false);
  }
}

async function removeDocument() {
  if (busy || !docId) return;
  showError("");
  setBusy(true);
  els.summary.textContent = "Waiting for the pipeline…";
  const stop = animateWaiting();
  try {
    const deleted = await api("DELETE", `/documents/${docId}`);
    const status = await waitForIndex(docId, deleted.version);
    stop();
    const timings = drawTimeline(status, true);
    const strong = document.createElement("strong");
    strong.textContent = formatMs(timings.indexed);
    els.summary.replaceChildren(
      "The delete reached the search index ",
      strong,
      " after it was saved. Search no longer returns any of its text.",
    );
    addHistory(status.indexed_version, "Deleted", timings, null, null);
    drawChunks(status);
    forget();
  } catch (error) {
    stop();
    resetTimeline();
    showError(`Could not delete: ${error.message}`);
  } finally {
    setBusy(false);
  }
}

/* Editor modes */

function forget() {
  docId = null;
  remember(null);
  lastSaved = { title: "", content: "" };
  els.title.value = SAMPLE_TITLE;
  els.content.value = SAMPLE;
  els.save.textContent = "Create document";
  els.del.hidden = true;
  els.presets.hidden = true;
  els.docVersion.textContent = "";
  showResultsMessage("Create the document first, then search it.");
}

function showEditing() {
  els.save.textContent = "Save changes";
  els.del.hidden = false;
  els.presets.hidden = false;
  syncPresetLabels();
}

const paragraphs = () =>
  els.content.value
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter(Boolean);

function syncPresetLabels() {
  const add = els.presets.querySelector('[data-preset="add"]');
  add.textContent = els.content.value.includes(MILEAGE)
    ? "Remove the paragraph about mileage"
    : "Add a paragraph about mileage";
  const price = els.presets.querySelector('[data-preset="price"]');
  const match = els.content.value.match(/nightly limit of (\d+) dollars/);
  price.hidden = !match;
  if (match) price.textContent = `Raise the hotel limit to ${Number(match[1]) + 50} dollars`;
}

const PRESETS = {
  price() {
    let raised = 0;
    els.content.value = els.content.value.replace(/nightly limit of (\d+) dollars/, (_, n) => {
      raised = Number(n) + 50;
      return `nightly limit of ${raised} dollars`;
    });
    return `Raised the hotel limit to ${raised} dollars`;
  },
  add() {
    const parts = paragraphs();
    if (parts.includes(MILEAGE)) {
      els.content.value = parts.filter((p) => p !== MILEAGE).join("\n\n");
      return "Removed the mileage paragraph";
    }
    parts.splice(Math.min(2, parts.length), 0, MILEAGE);
    els.content.value = parts.join("\n\n");
    return "Added a paragraph about mileage";
  },
  move() {
    const parts = paragraphs();
    parts.unshift(parts.pop());
    els.content.value = parts.join("\n\n");
    return "Moved the last paragraph to the top";
  },
};

/* Start */

async function restore() {
  const id = remembered();
  if (!id) return forget();
  try {
    const [doc, status] = await Promise.all([
      api("GET", `/documents/${id}`),
      api("GET", `/documents/${id}/index`),
    ]);
    docId = id;
    els.title.value = doc.title;
    els.content.value = doc.content;
    lastSaved = { title: doc.title, content: doc.content };
    els.docVersion.textContent = `Version ${doc.version} saved`;
    drawChunks(status);
    showEditing();
    if (status.indexed_at && status.indexed_version === doc.version) drawTimeline(status, false);
    els.summary.textContent = `Your document from last time, at version ${doc.version}. Make a change to time it.`;
    runSearch();
  } catch {
    forget();
  }
}

els.save.addEventListener("click", () => save());
els.del.addEventListener("click", removeDocument);
els.presets.addEventListener("click", (event) => {
  const button = event.target.closest("[data-preset]");
  if (!button || busy) return;
  const change = PRESETS[button.dataset.preset]();
  save(change);
});
els.content.addEventListener("input", syncPresetLabels);
els.search.addEventListener("submit", (event) => {
  event.preventDefault();
  runSearch();
});

resetTimeline();
restore();
