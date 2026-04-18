// Simple, bulletproof PDF viewer:
// - Server renders the cited page to a PNG with the snippet highlighted
//   (pymupdf finds the phrase's bounding boxes and draws an amber highlight
//   annotation before rasterizing). The client just displays an <img>.
// - Prev/Next fetches a different page (no snippet highlight on pages
//   other than the cited one, since the model grounded it specifically).
// - Zoom in/out re-fetches at a larger/smaller scale.
//
// This replaces the previous pdf.js-based viewer, which repeatedly failed
// to position highlights reliably on table-layout and numeric-dense pages.

const overlay = document.getElementById("pdf-overlay");
const imgEl = document.getElementById("pdf-page-img");
const statusEl = document.getElementById("pdf-status");
const subtitleEl = document.getElementById("pdf-subtitle");
const searchBanner = document.getElementById("pdf-search-banner");
const pageInfo = document.getElementById("pdf-pageinfo");
const prevBtn = document.getElementById("pdf-prev");
const nextBtn = document.getElementById("pdf-next");
const zoomIn = document.getElementById("pdf-zoom-in");
const zoomOut = document.getElementById("pdf-zoom-out");
const openExt = document.getElementById("pdf-open-ext");

let state = {
  pdfPath: "",
  citedPage: 1,
  currentPage: 1,
  numPages: 1,
  snippet: "",
  scale: 1.6,
  title: "",
};

function setStatus(msg, show) {
  statusEl.textContent = msg || "";
  statusEl.style.display = show ? "flex" : "none";
}

function updatePageInfo() {
  pageInfo.textContent = `${state.currentPage} / ${state.numPages}`;
  prevBtn.disabled = state.currentPage <= 1;
  nextBtn.disabled = state.currentPage >= state.numPages;
}

async function renderCurrent() {
  if (!state.pdfPath) {
    setStatus("No PDF path", true);
    return;
  }
  setStatus("Loading page…", true);
  const params = new URLSearchParams({
    path: state.pdfPath,
    page: String(state.currentPage),
    scale: String(state.scale),
  });
  // Only pass the snippet when we're on the page the model actually cited —
  // showing a phantom highlight while the user flips through unrelated pages
  // would be misleading.
  if (state.currentPage === state.citedPage && state.snippet) {
    params.set("snippet", state.snippet);
  }
  const fetchUrl = `/api/pdf-render?${params.toString()}`;
  console.log("[pdf-viewer] src ->", fetchUrl);

  // One HEAD-ish fetch to learn the total page count from headers (this was
  // the only reason we previously used fetch()+blob). Cached response on the
  // server, and the browser will reuse it for the <img> request thanks to
  // HTTP caching, so it's not a wasteful duplicate request.
  try {
    const res = await fetch(fetchUrl, { credentials: "include", method: "HEAD" });
    const numPagesHdr = parseInt(res.headers.get("X-Pdf-Num-Pages") || "0", 10);
    if (numPagesHdr > 0) state.numPages = numPagesHdr;
  } catch (e) {
    console.warn("[pdf-viewer] HEAD failed, continuing without num-pages update", e);
  }

  imgEl.onload = () => {
    console.log("[pdf-viewer] img loaded", imgEl.naturalWidth, "x", imgEl.naturalHeight, "| bbox:", imgEl.getBoundingClientRect());
    setStatus("", false);
  };
  imgEl.onerror = (ev) => {
    console.error("[pdf-viewer] img error", ev);
    setStatus("Failed to load page image", true);
  };
  imgEl.src = fetchUrl;
  updatePageInfo();
}

async function openPdfViewer({ pdfUrl, page, snippet, title, openExternal, pdfPath }) {
  // Accept either an explicit volume path or derive it from the /api/pdf URL
  // we were using before the server-render switch.
  let path = pdfPath || "";
  if (!path && pdfUrl) {
    try {
      const u = new URL(pdfUrl, window.location.origin);
      path = u.searchParams.get("path") || "";
    } catch {
      path = "";
    }
  }

  overlay.hidden = false;
  document.body.style.overflow = "hidden";

  state.pdfPath = path;
  state.citedPage = Math.max(1, parseInt(page, 10) || 1);
  state.currentPage = state.citedPage;
  state.snippet = snippet || "";
  state.scale = 1.6;
  state.numPages = 1;
  state.title = title || "";

  subtitleEl.textContent = state.title
    ? `${state.title}${state.citedPage ? ` · page ${state.citedPage}` : ""}`
    : "";
  openExt.href = openExternal || "#";
  openExt.style.display = openExternal ? "inline-flex" : "none";

  if (state.snippet) {
    searchBanner.textContent = `Highlighted passage from the model's citation`;
    searchBanner.hidden = false;
  } else {
    searchBanner.hidden = true;
  }
  pageInfo.textContent = "—";
  prevBtn.disabled = true;
  nextBtn.disabled = true;
  imgEl.removeAttribute("src");

  await renderCurrent();
}

function closePdfViewer() {
  overlay.hidden = true;
  document.body.style.overflow = "";
  if (imgEl.dataset.blobUrl) {
    URL.revokeObjectURL(imgEl.dataset.blobUrl);
    delete imgEl.dataset.blobUrl;
  }
  imgEl.removeAttribute("src");
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
  if (state.currentPage > 1) {
    state.currentPage -= 1;
    renderCurrent();
  }
});
nextBtn.addEventListener("click", () => {
  if (state.currentPage < state.numPages) {
    state.currentPage += 1;
    renderCurrent();
  }
});
zoomIn.addEventListener("click", () => {
  state.scale = Math.min(3.0, state.scale + 0.25);
  renderCurrent();
});
zoomOut.addEventListener("click", () => {
  state.scale = Math.max(0.75, state.scale - 0.25);
  renderCurrent();
});

window.openPdfViewer = openPdfViewer;
