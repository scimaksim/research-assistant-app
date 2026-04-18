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

function cleanSnippet(s) {
  return String(s || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
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

function sourceLabel(c) {
  const t = reportTitle(c);
  return c.report_id ? `${t} (${c.report_id})` : t;
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
  const cleanSnip = cleanSnippet(c.snippet);
  const viewBtnLabel = c.synthesized ? "Open PDF" : "View page";
  const viewBtnSnippet = c.synthesized ? "" : cleanSnip;
  const viewBtn = proxyPath
    ? `<button class="pdf-view" type="button" data-path="${encodeURI(proxyPath)}" data-page="${c.page != null ? escapeHtml(c.page) : ""}" data-snippet="${escapeHtml(viewBtnSnippet)}" data-title="${escapeHtml(reportTitle(c))}" data-ext="${pdfHref ? encodeURI(pdfHref) : ""}">${viewBtnLabel}</button>`
    : "";
  const pdfLink = pdfHref
    ? `<a class="pdf-link" href="${encodeURI(pdfHref)}" target="_blank" rel="noreferrer">Open volume in Databricks ↗</a>`
    : "";
  const snippet = highlight(cleanSnip, userQuery);
  const secRow = secTag ? `<div class="cite-tags">${secTag}</div>` : "";
  const bodyHtml = c.synthesized
    ? `<div class="snippet"><em>Referenced in the answer. No retrieval snippet — this answer came from the metadata route (Genie). Open the PDF to read the full report.</em></div>`
    : `${secRow}<div class="snippet">${snippet || "<em>(no snippet)</em>"}</div>`;
  card.innerHTML = `
    <div class="cite-head">
      <span class="num-badge">${num}</span>
      <div class="title-block">
        <div class="title">${escapeHtml(title)}${synthTag}</div>
        ${c.report_id ? `<div class="title-meta"><code>${escapeHtml(c.report_id)}</code>${c.page != null ? ` · page ${escapeHtml(c.page)}` : ""}</div>` : ""}
      </div>
    </div>
    <div class="cite-body">
      ${bodyHtml}
    </div>
    <div class="cite-foot">
      <span></span>
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
    const label = sourceLabel(c);
    const page = c.page != null ? ` · p.${escapeHtml(c.page)}` : "";
    const synth = c.synthesized ? ' <span class="src-synth">inferred</span>' : "";
    const inner = `<span class="src-num">${num}</span><span class="src-label-text">${escapeHtml(label)}${page}${synth}</span>`;
    if (!c.doc_uri) {
      return `<span class="src-pill" data-cite="${num}" title="${escapeHtml(label)}">${inner}</span>`;
    }
    // Synthesized citations (from Genie-path answers) don't have page/snippet
    // grounding, but we still know which PDF to open — let the user click
    // through to the document rather than a dead span.
    const snippetAttr = c.synthesized ? "" : escapeHtml(cleanSnippet(c.snippet));
    const pageAttr = c.page != null ? escapeHtml(c.page) : "";
    return `<button type="button" class="src-pill" data-cite="${num}" title="${escapeHtml(label)}" data-path="${encodeURI(c.doc_uri)}" data-page="${pageAttr}" data-snippet="${snippetAttr}" data-title="${escapeHtml(reportTitle(c))}" data-ext="${c.volume_url ? encodeURI(c.volume_url) : ""}">${inner}</button>`;
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

// Click a [1] reference in answer -> open the PDF viewer straight to the
// cited page; also flash the sidebar card so it's clear which source was used.
document.addEventListener("click", (e) => {
  const ref = e.target.closest("a.cite-ref");
  if (ref) {
    e.preventDefault();
    const n = parseInt(ref.dataset.cite, 10);
    const card = document.getElementById(`cite-${n}`);
    if (card) {
      card.classList.remove("flash");
      void card.offsetWidth;
      card.classList.add("flash");
    }
    const c = currentCitations[n - 1];
    // Open the PDF for ANY citation that has a doc_uri — even synthesized ones
    // (those come from report_ids in the answer, so we know the file exists
    // even if we don't have a specific page/snippet to highlight).
    if (c && c.doc_uri && typeof window.openPdfViewer === "function") {
      const page = c.page != null ? parseInt(c.page, 10) || 1 : 1;
      window.openPdfViewer({
        pdfUrl: `/api/pdf?path=${encodeURIComponent(c.doc_uri)}`,
        page,
        snippet: c.synthesized ? "" : cleanSnippet(c.snippet),
        title: reportTitle(c),
        openExternal: c.volume_url || "",
      });
    } else if (card) {
      card.scrollIntoView({ behavior: "smooth", block: "center" });
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
