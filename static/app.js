const messagesEl = document.getElementById("messages");
const inputEl = document.getElementById("input");
const sendBtn = document.getElementById("send");
const routeSel = document.getElementById("route");
const citationsEl = document.getElementById("citations");
const citationsCount = document.getElementById("citations-count");
const composer = document.getElementById("composer");

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

function linkifyCitations(text) {
  // Replace [1], [2] etc. with anchor tags that scroll to the citation card
  return escapeHtml(text).replace(/\[(\d+)\]/g, (match, num) => {
    const n = parseInt(num, 10);
    if (n >= 1 && n <= currentCitations.length) {
      return `<a class="cite-ref" href="#cite-${n}" data-cite="${n}">${n}</a>`;
    }
    return match;
  });
}

function renderCitations(cits, userQuery) {
  currentCitations = cits || [];
  citationsCount.textContent = cits && cits.length ? `${cits.length} source${cits.length > 1 ? "s" : ""}` : "";
  if (!cits || cits.length === 0) {
    citationsEl.innerHTML = '<p class="empty">Ask a content question (gold forecast, sector views, summaries) and citations will appear here — report, page, section, paragraph — so you can visually verify the answer.</p>';
    return;
  }
  citationsEl.innerHTML = "";
  cits.forEach((c, i) => {
    const num = i + 1;
    const card = document.createElement("div");
    card.className = "cite";
    card.id = `cite-${num}`;
    const title = reportTitle(c);
    const pageTag = c.page != null ? `<span class="page-tag">page ${escapeHtml(c.page)}</span>` : "";
    const secTag = c.section ? `<span class="sec-tag">§ ${escapeHtml(c.section)}</span>` : "";
    const pdfHref = c.volume_url || c.doc_uri;
    const pdfLink = pdfHref
      ? `<a class="pdf-link" href="${encodeURI(pdfHref)}" target="_blank" rel="noreferrer">Open PDF${c.page != null ? ` at page ${escapeHtml(c.page)}` : ""} ↗</a>`
      : "";
    const snippet = highlight(c.snippet || "", userQuery);
    card.innerHTML = `
      <div class="cite-head">
        <span class="num-badge">${num}</span>
        <div class="title">${escapeHtml(title)}</div>
      </div>
      <div class="cite-body">
        <div>${pageTag}${secTag}</div>
        <div class="snippet">${snippet || "<em>(no snippet)</em>"}</div>
      </div>
      <div class="cite-foot">
        <span>${c.report_id ? `<code>${escapeHtml(c.report_id)}</code>` : ""}</span>
        ${pdfLink}
      </div>
    `;
    citationsEl.appendChild(card);
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
    const answerHtml = linkifyCitations(data.answer || "(no content)");
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
  if (!ref) return;
  e.preventDefault();
  const n = ref.dataset.cite;
  const card = document.getElementById(`cite-${n}`);
  if (card) {
    card.scrollIntoView({ behavior: "smooth", block: "center" });
    card.classList.remove("flash");
    void card.offsetWidth;
    card.classList.add("flash");
  }
});

// Greeting
addBubble(
  "assistant",
  "Hi — ask me anything about the research report library. I'll route metadata questions (latest, authors, teams) to Genie and content questions (forecasts, sector views) to the Knowledge Assistant. Citations show the exact page and section you can click into.",
);
