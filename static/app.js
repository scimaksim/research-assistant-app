const messagesEl = document.getElementById("messages");
const inputEl = document.getElementById("input");
const sendBtn = document.getElementById("send");
const routeSel = document.getElementById("route");
const citationsEl = document.getElementById("citations");
const citationsCount = document.getElementById("citations-count");
const composer = document.getElementById("composer");
const compareBtn = document.getElementById("compare-btn");
const compareOverlay = document.getElementById("compare-overlay");
const compareRow = document.getElementById("compare-row");

const history = [];

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const STOPWORDS = new Set([
  "what","which","where","when","does","their","there","have","with","this","that","from","about","into","over","under","some","most","many","than","then","only","also","will","would","could","should","been","were","they","your","yours","latest","newest","recent","summary","summarize","please","outlook","tell","give","show","value","values","number","numbers","report","reports",
]);
function highlight(paragraph, query) {
  const text = escapeHtml(paragraph);
  if (!query) return text;
  const terms = query
    .toLowerCase()
    .split(/\W+/)
    .filter((t) => t.length > 3 && !STOPWORDS.has(t))
    .slice(0, 10);
  if (!terms.length) return text;
  const pattern = new RegExp(`(${terms.map(escapeRegex).join("|")})`, "gi");
  return text.replace(pattern, "<mark>$1</mark>");
}

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function reportTitle(c) {
  if (c.title && c.title.length > 4 && !/^\d+R\d+\.pdf$/i.test(c.title)) return c.title;
  if (c.report_id) return `Report ${c.report_id}`;
  return "Source document";
}

function addBubble(role, html, extraClass = "") {
  const div = document.createElement("div");
  div.className = `bubble ${role} ${extraClass}`.trim();
  div.innerHTML = html;
  messagesEl.appendChild(div);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return div;
}

let currentCitations = [];
let currentQuery = "";

function buildCitationCard(c, num, userQuery) {
  const card = document.createElement("div");
  card.className = `cite${c.synthesized ? " cite-synth" : ""}`;
  card.id = `cite-${num}`;
  const title = reportTitle(c);
  const pageTag = c.page != null ? `<span class="page-tag">page ${escapeHtml(c.page)}</span>` : "";
  const secTag = c.section ? `<span class="sec-tag">§ ${escapeHtml(c.section)}</span>` : "";
  const synthTag = c.synthesized ? '<span class="synth-tag" title="Extracted from answer text">inferred</span>' : "";
  const pdfHref = c.volume_url || c.doc_uri;
  const proxyPath = c.doc_uri || "";
  const viewBtn = proxyPath && !c.synthesized
    ? `<button class="pdf-view" type="button" data-path="${encodeURI(proxyPath)}" data-page="${c.page != null ? escapeHtml(c.page) : ""}" data-snippet="${escapeHtml(c.snippet || "")}" data-title="${escapeHtml(reportTitle(c))}" data-ext="${pdfHref ? encodeURI(pdfHref) : ""}">View page</button>`
    : "";
  const pdfLink = pdfHref
    ? `<a class="pdf-link" href="${encodeURI(pdfHref)}" target="_blank" rel="noreferrer">Open in Databricks${c.page != null ? ` at page ${escapeHtml(c.page)}` : ""} ↗</a>`
    : "";
  const snippet = highlight(c.snippet || "", userQuery);
  const bodyHtml = c.synthesized
    ? `<div class="snippet"><em>Referenced in the answer. No retrieval snippet — this answer came from the metadata route (Genie). Open the PDF to read the full report.</em></div>`
    : `<div>${pageTag}${secTag}</div><div class="snippet">${snippet || "<em>(no snippet)</em>"}</div>`;
  card.innerHTML = `
    <div class="cite-head">
      <span class="num-badge">${num}</span>
      <div class="title">${escapeHtml(title)}${synthTag}</div>
    </div>
    <div class="cite-body">
      ${bodyHtml}
    </div>
    <div class="cite-foot">
      <span>${c.report_id ? `<code>${escapeHtml(c.report_id)}</code>` : ""}</span>
      <span class="cite-foot-actions">${viewBtn}${pdfLink}</span>
    </div>
  `;
  return card;
}

function openCompare() {
  if (!currentCitations.length) return;
  compareRow.innerHTML = "";
  currentCitations.forEach((c, i) => {
    compareRow.appendChild(buildCitationCard(c, i + 1, currentQuery));
  });
  compareOverlay.hidden = false;
  document.body.style.overflow = "hidden";
}

function closeCompare() {
  compareOverlay.hidden = true;
  document.body.style.overflow = "";
}

compareBtn.addEventListener("click", openCompare);
compareOverlay.addEventListener("click", (e) => {
  if (e.target.dataset.close === "1") closeCompare();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && !compareOverlay.hidden) closeCompare();
});

function renderAnswer(text) {
  // Markdown -> sanitized HTML, then turn [1] / [2] into clickable citation refs
  const raw = typeof marked !== "undefined"
    ? marked.parse(text, { breaks: true, gfm: true })
    : escapeHtml(text).replace(/\n/g, "<br>");
  const clean = typeof DOMPurify !== "undefined" ? DOMPurify.sanitize(raw) : raw;
  return clean.replace(/\[(\d+)\]/g, (match, num) => {
    const n = parseInt(num, 10);
    if (n >= 1 && n <= currentCitations.length) {
      const c = currentCitations[n - 1];
      const label = reportTitle(c) + (c.page != null ? ` · page ${c.page}` : "");
      return `<a class="cite-ref" href="#cite-${n}" data-cite="${n}" title="${escapeHtml(label)}">${n}</a>`;
    }
    return match;
  });
}

function renderSourceStrip(cits) {
  if (!cits || !cits.length) return "";
  const pills = cits.map((c, i) => {
    const num = i + 1;
    const title = reportTitle(c);
    const page = c.page != null ? ` · p.${escapeHtml(c.page)}` : "";
    const synth = c.synthesized ? ' <span class="src-synth">inferred</span>' : "";
    const inner = `<span class="src-num">${num}</span>${escapeHtml(title)}${page}${synth}`;
    if (c.synthesized || !c.doc_uri) {
      return `<span class="src-pill" data-cite="${num}">${inner}</span>`;
    }
    return `<button type="button" class="src-pill" data-cite="${num}" data-path="${encodeURI(c.doc_uri)}" data-page="${c.page != null ? escapeHtml(c.page) : ""}" data-snippet="${escapeHtml(c.snippet || "")}" data-title="${escapeHtml(title)}" data-ext="${c.volume_url ? encodeURI(c.volume_url) : ""}">${inner}</button>`;
  }).join("");
  return `<div class="source-strip"><span class="src-label">Source${cits.length > 1 ? "s" : ""}:</span>${pills}</div>`;
}

function renderCitations(cits, userQuery) {
  currentCitations = cits || [];
  currentQuery = userQuery || "";
  const n = currentCitations.length;
  citationsCount.textContent = n ? `${n} source${n > 1 ? "s" : ""}` : "";
  compareBtn.disabled = n < 2;
  compareBtn.title = n < 2 ? "Compare requires 2 or more sources" : "Open side-by-side comparison";
  if (!n) {
    citationsEl.innerHTML = '<p class="empty">Ask a content question (gold forecast, sector views, summaries) and citations will appear here — report, page, section, paragraph — so you can visually verify the answer.</p>';
    return;
  }
  citationsEl.innerHTML = "";
  currentCitations.forEach((c, i) => {
    citationsEl.appendChild(buildCitationCard(c, i + 1, userQuery));
  });
}

async function sendMessage(text) {
  if (!text.trim()) return;
  addBubble("user", escapeHtml(text));
  history.push({ role: "user", content: text });
  sendBtn.disabled = true;

  const pending = document.createElement("div");
  pending.className = "bubble assistant";
  pending.innerHTML = '<span class="spin"></span>Thinking…';
  messagesEl.appendChild(pending);
  messagesEl.scrollTop = messagesEl.scrollHeight;

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: text,
        route: routeSel.value,
        history: history.slice(0, -1),
      }),
    });
    const data = await res.json();
    pending.remove();

    const cits = data.citations || [];
    currentCitations = cits;
    const answerHtml = renderAnswer(data.answer || "(no content)") + renderSourceStrip(cits);
    addBubble("assistant", answerHtml);
    history.push({ role: "assistant", content: data.answer || "" });
    renderCitations(cits, text);

    addBubble("meta", `served by <code>${escapeHtml(data.endpoint)}</code>`, "meta");
  } catch (err) {
    pending.remove();
    addBubble("assistant", `Error: ${escapeHtml(err.message)}`, "meta");
  } finally {
    sendBtn.disabled = false;
    inputEl.focus();
  }
}

composer.addEventListener("submit", (e) => {
  e.preventDefault();
  const v = inputEl.value.trim();
  if (!v) return;
  inputEl.value = "";
  sendMessage(v);
});

document.getElementById("chips").addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-q]");
  if (!btn) return;
  sendMessage(btn.dataset.q);
});

inputEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    composer.requestSubmit();
  }
});

// Click a [1] reference in answer -> scroll + flash the citation card
document.addEventListener("click", (e) => {
  const ref = e.target.closest("a.cite-ref");
  if (ref) {
    e.preventDefault();
    const n = ref.dataset.cite;
    const card = document.getElementById(`cite-${n}`);
    if (card) {
      card.scrollIntoView({ behavior: "smooth", block: "center" });
      card.classList.remove("flash");
      void card.offsetWidth;
      card.classList.add("flash");
    }
    return;
  }

  const viewBtn = e.target.closest("button.pdf-view, button.src-pill");
  if (viewBtn && typeof window.openPdfViewer === "function") {
    e.preventDefault();
    const path = decodeURI(viewBtn.dataset.path || "");
    if (!path) return;
    const pageRaw = viewBtn.dataset.page || "";
    const page = pageRaw ? parseInt(pageRaw, 10) : 1;
    const snippet = viewBtn.dataset.snippet || "";
    const title = viewBtn.dataset.title || "";
    const openExternal = viewBtn.dataset.ext ? decodeURI(viewBtn.dataset.ext) : "";
    window.openPdfViewer({
      pdfUrl: `/api/pdf?path=${encodeURIComponent(path)}`,
      page,
      snippet,
      title,
      openExternal,
    });
  }
});

// Greeting
addBubble(
  "assistant",
  "Hi — ask me anything about the research report library. I'll route metadata questions (latest, authors, teams) to Genie and content questions (forecasts, sector views) to the Knowledge Assistant. Citations show the exact page and section you can click into.",
);
