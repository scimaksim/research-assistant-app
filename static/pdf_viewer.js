import * as pdfjsLib from "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.7.76/build/pdf.min.mjs";

pdfjsLib.GlobalWorkerOptions.workerSrc =
  "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.7.76/build/pdf.worker.min.mjs";

const overlay = document.getElementById("pdf-overlay");
const pageEl = document.getElementById("pdf-page");
const statusEl = document.getElementById("pdf-status");
const subtitleEl = document.getElementById("pdf-subtitle");
const pageInfo = document.getElementById("pdf-pageinfo");
const prevBtn = document.getElementById("pdf-prev");
const nextBtn = document.getElementById("pdf-next");
const zoomIn = document.getElementById("pdf-zoom-in");
const zoomOut = document.getElementById("pdf-zoom-out");
const openExt = document.getElementById("pdf-open-ext");

let currentDoc = null;
let currentPage = 1;
let currentScale = 1.4;
let currentHighlight = "";
let renderTask = null;

function setStatus(text, visible) {
  statusEl.textContent = text || "";
  statusEl.style.display = visible ? "flex" : "none";
}

function pickHighlightTerms(snippet) {
  if (!snippet) return [];
  const cleaned = String(snippet)
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const stop = new Set([
    "the","and","for","with","from","that","this","into","over","their","they",
    "were","been","have","has","was","are","our","its","but","not","than","then",
    "will","would","could","should","about","also","more","most","some","these",
    "those","which","what","where","when","there","here","such","per","via",
  ]);
  const tokens = cleaned
    .split(/\s+/)
    .map((t) => t.replace(/^[^\w$%.]+|[^\w$%.]+$/g, ""))
    .filter(Boolean);

  const scored = tokens
    .map((t) => {
      const lower = t.toLowerCase();
      if (stop.has(lower)) return null;
      let score = 0;
      if (/^\$?\d[\d.,%]*[a-z]{0,3}$/i.test(t)) score += 6;
      if (/^\d{4}$/.test(t)) score += 3;
      if (/^[A-Z]{2,}$/.test(t)) score += 2;
      if (t.length >= 6) score += 1;
      if (score === 0 && t.length < 5) return null;
      return { t, score };
    })
    .filter(Boolean);

  const seen = new Set();
  const out = [];
  for (const { t } of scored.sort((a, b) => b.score - a.score)) {
    const lower = t.toLowerCase();
    if (seen.has(lower)) continue;
    seen.add(lower);
    out.push(t);
    if (out.length >= 8) break;
  }
  return out;
}

function highlightTextLayer(container, terms) {
  if (!terms.length) return;
  const escaped = terms
    .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .sort((a, b) => b.length - a.length);
  const pattern = new RegExp(`(${escaped.join("|")})`, "gi");

  const spans = container.querySelectorAll("span");
  spans.forEach((span) => {
    const text = span.textContent;
    if (!text || !pattern.test(text)) return;
    pattern.lastIndex = 0;
    const frag = document.createDocumentFragment();
    let last = 0;
    text.replace(pattern, (match, _g, idx) => {
      if (idx > last) frag.appendChild(document.createTextNode(text.slice(last, idx)));
      const mark = document.createElement("mark");
      mark.className = "pdf-hl";
      mark.textContent = match;
      frag.appendChild(mark);
      last = idx + match.length;
      return match;
    });
    if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)));
    span.replaceChildren(frag);
  });
}

async function renderPage() {
  if (!currentDoc) return;
  pageInfo.textContent = `${currentPage} / ${currentDoc.numPages}`;
  prevBtn.disabled = currentPage <= 1;
  nextBtn.disabled = currentPage >= currentDoc.numPages;

  if (renderTask) {
    try { renderTask.cancel(); } catch {}
    renderTask = null;
  }
  setStatus("Rendering page…", true);
  pageEl.innerHTML = "";

  const page = await currentDoc.getPage(currentPage);
  const viewport = page.getViewport({ scale: currentScale });

  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  canvas.width = viewport.width * dpr;
  canvas.height = viewport.height * dpr;
  canvas.style.width = `${viewport.width}px`;
  canvas.style.height = `${viewport.height}px`;

  const wrap = document.createElement("div");
  wrap.className = "pdfPage";
  wrap.style.width = `${viewport.width}px`;
  wrap.style.height = `${viewport.height}px`;
  wrap.appendChild(canvas);

  const textLayer = document.createElement("div");
  textLayer.className = "textLayer";
  textLayer.style.width = `${viewport.width}px`;
  textLayer.style.height = `${viewport.height}px`;
  wrap.appendChild(textLayer);
  pageEl.appendChild(wrap);

  renderTask = page.render({
    canvasContext: ctx,
    viewport: page.getViewport({ scale: currentScale * dpr }),
    transform: null,
  });
  try {
    await renderTask.promise;
  } catch (e) {
    if (e?.name !== "RenderingCancelledException") throw e;
    return;
  }

  const textContent = await page.getTextContent();
  await pdfjsLib.renderTextLayer({
    textContentSource: textContent,
    container: textLayer,
    viewport,
    textDivs: [],
  }).promise;

  const terms = pickHighlightTerms(currentHighlight);
  highlightTextLayer(textLayer, terms);

  setStatus("", false);
  const firstHl = textLayer.querySelector(".pdf-hl");
  if (firstHl) firstHl.scrollIntoView({ block: "center", behavior: "smooth" });
}

async function openPdfViewer({ pdfUrl, page, snippet, title, openExternal }) {
  overlay.hidden = false;
  document.body.style.overflow = "hidden";
  subtitleEl.textContent = title ? `${title}${page ? ` · page ${page}` : ""}` : "";
  openExt.href = openExternal || "#";
  openExt.style.display = openExternal ? "inline-flex" : "none";
  setStatus("Loading PDF…", true);
  pageEl.innerHTML = "";
  currentPage = page || 1;
  currentScale = 1.4;
  currentHighlight = snippet || "";

  try {
    const task = pdfjsLib.getDocument({ url: pdfUrl, withCredentials: true });
    currentDoc = await task.promise;
    if (currentPage < 1) currentPage = 1;
    if (currentPage > currentDoc.numPages) currentPage = currentDoc.numPages;
    await renderPage();
  } catch (e) {
    setStatus(`Failed to load PDF: ${e.message || e}`, true);
  }
}

function closePdfViewer() {
  overlay.hidden = true;
  document.body.style.overflow = "";
  if (renderTask) {
    try { renderTask.cancel(); } catch {}
    renderTask = null;
  }
  if (currentDoc) {
    try { currentDoc.destroy(); } catch {}
    currentDoc = null;
  }
  pageEl.innerHTML = "";
}

overlay.addEventListener("click", (e) => {
  if (e.target.dataset.pdfClose === "1") closePdfViewer();
});
document.addEventListener("keydown", (e) => {
  if (overlay.hidden) return;
  if (e.key === "Escape") closePdfViewer();
  if (e.key === "ArrowLeft") prevBtn.click();
  if (e.key === "ArrowRight") nextBtn.click();
});
prevBtn.addEventListener("click", () => {
  if (currentDoc && currentPage > 1) { currentPage -= 1; renderPage(); }
});
nextBtn.addEventListener("click", () => {
  if (currentDoc && currentPage < currentDoc.numPages) { currentPage += 1; renderPage(); }
});
zoomIn.addEventListener("click", () => {
  currentScale = Math.min(currentScale + 0.2, 3.0);
  renderPage();
});
zoomOut.addEventListener("click", () => {
  currentScale = Math.max(currentScale - 0.2, 0.6);
  renderPage();
});

window.openPdfViewer = openPdfViewer;
