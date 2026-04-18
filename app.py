import io
import os
import re
import time
import urllib.parse
from typing import Any, Optional

import fitz  # pymupdf — server-side PDF rendering + text search
import httpx
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

SUPERVISOR_ENDPOINT = os.environ.get("SUPERVISOR_ENDPOINT", "mas-d45b0de3-endpoint")
KA_ENDPOINT = os.environ.get("KA_ENDPOINT", "ka-77835fba-endpoint")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f13a85f3dd12ba954cc350e5092f74")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "8baced1ff014912d")
VOLUME_ROOT = os.environ.get("VOLUME_ROOT", "/Volumes/research_assistant_demo/default/research_pdfs")

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")

app = FastAPI(title="Research Assistant")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


class ChatRequest(BaseModel):
    message: str
    route: Optional[str] = "supervisor"
    history: Optional[list[dict[str, Any]]] = None


def _auth_headers() -> dict[str, str]:
    auth = w.config.authenticate()
    return {"Authorization": auth.get("Authorization", ""), "Content-Type": "application/json"}


def _volume_browser_url(doc_path: str, page: Optional[int] = None) -> str:
    """Build a URL that opens the containing Unity Catalog volume in the
    Databricks UI file browser. We intentionally link to the VOLUME ROOT
    (not the file itself) because the per-file Databricks UI URL pattern is
    not reliably handled across workspace UI versions — it 404s in some —
    whereas the volume root is a stable, guaranteed deep link. The user can
    click the specific file from the volume listing once they land there."""
    parts = (doc_path or "").lstrip("/").split("/")
    if len(parts) >= 5 and parts[0] == "Volumes":
        catalog, schema, volume = parts[1], parts[2], parts[3]
        return f"{HOST}/explore/data/volumes/{catalog}/{schema}/{volume}"
    return doc_path


def _parse_annotation(url: str) -> dict[str, Any]:
    out: dict[str, Any] = {"doc_uri": "", "page": None, "snippet": "", "report_id": None}
    if not url:
        return out
    m_path = re.search(r"/Volumes/([^#?]+)", url)
    if m_path:
        doc_uri = "/Volumes/" + m_path.group(1)
        out["doc_uri"] = doc_uri
        rid = re.search(r"([0-9]+R[0-9]+)\.pdf", doc_uri)
        if rid:
            out["report_id"] = rid.group(1)
    frag = url.split("#", 1)[1] if "#" in url else ""
    m_page = re.search(r"page=(\d+)", frag)
    if m_page:
        try:
            out["page"] = int(m_page.group(1))
        except ValueError:
            pass
    m_text = re.search(r":~:text=([^&]*)", frag)
    if m_text:
        raw = m_text.group(1)
        try:
            snippet = urllib.parse.unquote_plus(raw).replace("\u2028", " ").replace("\n", " ").strip()
        except Exception:
            snippet = raw
        snippet = re.sub(r"\s+", " ", snippet)
        out["snippet"] = snippet[:700]
    return out


def _looks_internal(text: str) -> bool:
    s = (text or "").strip()
    if s.startswith("<name>") and s.endswith("</name>"):
        return True
    if re.fullmatch(r"<[^>]+>", s):
        return True
    return False


def _query_endpoint(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{HOST}/serving-endpoints/{endpoint}/invocations"
    try:
        r = httpx.post(url, headers=_auth_headers(), json=payload, timeout=120.0)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"endpoint error: {e}") from e
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text[:2000])
    return r.json()


def _build_input(req: ChatRequest) -> list[dict[str, Any]]:
    msgs: list[dict[str, Any]] = []
    if req.history:
        for h in req.history[-10:]:
            if h.get("role") and h.get("content"):
                msgs.append({"role": h["role"], "content": h["content"]})
    msgs.append({"role": "user", "content": req.message})
    return msgs


def _parse_response(resp: dict[str, Any]) -> dict[str, Any]:
    """Parse the responses-API shape used by Agent Bricks serving endpoints.

    Also collects url_citation annotations WITH their character offsets so the
    caller can inject inline [N] markers into the answer text at the exact
    positions the model grounded its claims."""
    final_text_parts: list[str] = []
    intermediate_text: list[str] = []
    citations: list[dict[str, Any]] = []
    tools_called: list[str] = []
    # anchors: (char_offset_in_final_text, citation_key) — citation_key is the
    # (doc_uri, page, snippet-prefix) tuple so we can map it to the final
    # citation index after dedup.
    anchors: list[tuple[int, tuple]] = []
    seen: dict[tuple, int] = {}

    output = resp.get("output") or []
    message_items = [(i, item) for i, item in enumerate(output) if isinstance(item, dict) and item.get("type") == "message"]

    for idx, item in enumerate(output):
        if not isinstance(item, dict):
            continue
        itype = item.get("type")
        if itype == "function_call":
            fn_name = item.get("name") or ""
            if fn_name:
                tools_called.append(fn_name)
            continue
        if itype == "message":
            is_final = (idx == message_items[-1][0]) if message_items else False
            content = item.get("content") or []
            text_pieces: list[str] = []
            # Track (relative_end_index, citation_key) for each content piece,
            # then shift to absolute positions once we know each piece's offset.
            piece_anchors: list[list[tuple[int, tuple]]] = []
            for c in content:
                if not isinstance(c, dict):
                    continue
                if c.get("type") in ("output_text", "text"):
                    t = c.get("text") or ""
                    if _looks_internal(t):
                        continue
                    text_pieces.append(t)
                    local_anchors: list[tuple[int, tuple]] = []
                    for ann in c.get("annotations") or []:
                        if not isinstance(ann, dict):
                            continue
                        if ann.get("type") != "url_citation":
                            continue
                        parsed = _parse_annotation(ann.get("url") or "")
                        if not parsed["doc_uri"] and not parsed["snippet"]:
                            continue
                        key = (parsed["doc_uri"], parsed["page"], parsed["snippet"][:80])
                        if key not in seen:
                            seen[key] = len(citations)
                            citations.append(
                                {
                                    "doc_uri": parsed["doc_uri"],
                                    "report_id": parsed["report_id"],
                                    "page": parsed["page"],
                                    "section": None,
                                    "snippet": parsed["snippet"],
                                    "volume_url": _volume_browser_url(parsed["doc_uri"], parsed["page"]),
                                    "title": ann.get("title") or "",
                                }
                            )
                        # Only attach anchors on the FINAL message — intermediate
                        # tool-chatter text gets discarded.
                        if is_final:
                            end_idx = ann.get("end_index")
                            if isinstance(end_idx, int) and end_idx >= 0:
                                local_anchors.append((end_idx, key))
                    piece_anchors.append(local_anchors)
            if is_final:
                # Compute absolute offsets. final_text is the newline-joined
                # accumulation across final messages; but inside a single
                # message, pieces are concatenated without a separator.
                base = sum(len(p) for p in final_text_parts)
                if final_text_parts:
                    base += 1  # for the "\n" joiner added later
                piece_offset = 0
                for text_piece, p_anchors in zip(text_pieces, piece_anchors):
                    for end_idx, key in p_anchors:
                        abs_pos = base + piece_offset + end_idx
                        anchors.append((abs_pos, key))
                    piece_offset += len(text_piece)
            joined = "".join(text_pieces).strip()
            if not joined:
                continue
            if is_final:
                final_text_parts.append(joined)
            else:
                intermediate_text.append(joined)

    final_text = "\n".join(final_text_parts).strip()
    if not final_text and intermediate_text:
        final_text = intermediate_text[-1]

    return {
        "answer": final_text,
        "citations": citations,
        "tools_called": tools_called,
        "anchors": anchors,
    }


_PARA_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "in", "on", "at", "to", "for", "with",
    "by", "from", "as", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "it", "its", "they", "their", "them",
    "we", "our", "us", "i", "you", "your", "there", "here", "which", "who",
    "whom", "whose", "what", "when", "where", "why", "how", "if", "then",
    "than", "so", "such", "not", "no", "also", "but", "however", "while",
    "about", "over", "under", "between", "among", "into", "through", "per",
    "report", "reports", "analyst", "research", "forecast", "forecasts",
}


def _tokens_for_match(text: str) -> set[str]:
    s = re.sub(r"<[^>]+>", " ", text or "")
    s = re.sub(r"[^\w%$.\-]+", " ", s.lower())
    toks = set()
    for t in s.split():
        t = t.strip(".,:;!?\"'()")
        if not t or len(t) < 3:
            continue
        if t in _PARA_STOPWORDS:
            continue
        toks.add(t)
    return toks


def _score_segment_against_citations(
    segment: str, cite_tokens: list[set[str]]
) -> tuple[int, float, int]:
    """Return (best_citation_index, score, numeric_hits_in_best). We track
    numeric hits separately so callers can require grounding on a concrete
    figure before attaching a marker."""
    ptoks = _tokens_for_match(segment)
    if len(ptoks) < 2:
        return (-1, 0.0, 0)
    best_idx = -1
    best_score = 0.0
    best_numeric = 0
    for i, ctoks in enumerate(cite_tokens):
        if not ctoks:
            continue
        overlap = ptoks & ctoks
        if not overlap:
            continue
        numeric_hits = sum(1 for t in overlap if re.search(r"[\d%$]", t))
        score = len(overlap) + 2.0 * numeric_hits
        if score > best_score:
            best_score = score
            best_idx = i
            best_numeric = numeric_hits
    return (best_idx, best_score, best_numeric)


# Split a block of prose into "sentence-ish" segments while preserving the
# delimiters so we can rejoin verbatim. Each returned tuple is (segment_text,
# trailing_delim). Markdown bullet/numbered list lines count as segments too.
# The lookahead ensures we only split on sentence-ending punctuation that's
# followed by whitespace or end-of-string — so "0.8%" and "$340bn" don't break.
_SEGMENT_SPLIT_RE = re.compile(
    r"([.!?]+(?=[\s\)\"']|$)[\s\)\"']*|\n(?:\s*[-*•]|\s*\d+\.)\s|\n)"
)


def _split_segments(text: str) -> list[tuple[str, str]]:
    parts = _SEGMENT_SPLIT_RE.split(text)
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(parts):
        seg = parts[i]
        delim = parts[i + 1] if i + 1 < len(parts) else ""
        if seg or delim:
            out.append((seg, delim))
        i += 2
    return out


def _heuristic_inject(answer: str, citations: list[dict[str, Any]]) -> str:
    """When the model didn't give us anchor offsets, match each sentence-ish
    segment in the answer to the citation whose snippet shares the most
    distinctive tokens, and insert [N] before the sentence's trailing
    punctuation. Tokens are lowered, stopwords-filtered, and include numeric
    content like "0.8%" / "$340bn". Only attach when overlap is meaningful so
    we don't staple markers onto generic intro/outro sentences."""
    if not answer or not citations:
        return answer
    cite_tokens = [_tokens_for_match(c.get("snippet") or "") for c in citations]
    if not any(cite_tokens):
        return answer

    segments = _split_segments(answer)
    if not segments:
        return answer

    out_parts: list[str] = []
    for seg, delim in segments:
        if not seg.strip():
            out_parts.append(seg + delim)
            continue
        # If segment already has a [N] marker, leave it alone.
        if re.search(r"\[\d+\]", seg):
            out_parts.append(seg + delim)
            continue
        best_idx, best_score, numeric_hits = _score_segment_against_citations(
            seg, cite_tokens
        )
        # Require EITHER a numeric/$/% token hit (the strongest grounding
        # signal — forecasts and figures), OR a dense multi-token overlap
        # (score >= 4 without numerics) — otherwise leave the sentence alone
        # to keep generic intro/outro/transition lines unmarked.
        if best_idx < 0 or (numeric_hits == 0 and best_score < 4):
            out_parts.append(seg + delim)
            continue
        marker = f" [{best_idx + 1}]"
        # If the delimiter starts with sentence-ending punctuation, insert the
        # marker between the segment body and the punctuation so it reads as
        # "... 2024 [1]." — otherwise append at end of segment body.
        if delim and re.match(r"[.!?]", delim):
            out_parts.append(seg.rstrip() + marker + seg[len(seg.rstrip()):] + delim)
        else:
            out_parts.append(seg.rstrip() + marker + seg[len(seg.rstrip()):] + delim)
    return "".join(out_parts)


def _inject_inline_markers(answer: str, anchors: list[tuple[int, tuple]], citations: list[dict[str, Any]], key_to_idx: dict[tuple, int]) -> str:
    """Insert [N] markers into the answer at the anchor positions. Positions
    are from the pre-dedup parse, but citation_key maps to the post-dedup
    index. Multiple anchors at the same sentence collapse to a single marker.
    """
    if not answer or not anchors:
        return answer
    # Dedup anchors: same position + same key = one marker.
    unique = {(pos, key) for pos, key in anchors if key in key_to_idx}
    # Sort descending so insertions don't shift earlier positions.
    ordered = sorted(unique, key=lambda x: -x[0])
    text = answer
    seen_pos: set[int] = set()
    for pos, key in ordered:
        if pos in seen_pos:
            continue
        seen_pos.add(pos)
        idx1 = key_to_idx[key] + 1
        marker = f" [{idx1}]"
        safe_pos = max(0, min(pos, len(text)))
        # Keep the marker adjacent to the preceding word; skip trailing
        # whitespace/punctuation so "... 2024.[1]" → "... 2024 [1]."
        insert_at = safe_pos
        while insert_at > 0 and text[insert_at - 1] in " \t\n":
            insert_at -= 1
        # Avoid inserting inside a number or word.
        if insert_at < len(text) and text[insert_at] in ".,:;!?)\"'":
            # Keep marker BEFORE trailing punctuation so it reads naturally.
            pass
        text = text[:insert_at] + marker + text[insert_at:]
    return text


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "supervisor": SUPERVISOR_ENDPOINT,
        "ka": KA_ENDPOINT,
        "genie_space": GENIE_SPACE_ID,
        "host": HOST,
    }


_TITLE_CACHE: Optional[dict[str, str]] = None


def _load_title_cache() -> dict[str, str]:
    """Lazy-load {report_id: title} from the metadata Delta table."""
    global _TITLE_CACHE
    if _TITLE_CACHE is not None:
        return _TITLE_CACHE
    try:
        body = {
            "warehouse_id": WAREHOUSE_ID,
            "statement": (
                "SELECT report_id, title FROM research_assistant_demo.default.report_metadata "
                "WHERE title IS NOT NULL AND title != ''"
            ),
            "wait_timeout": "30s",
        }
        r = httpx.post(
            f"{HOST}/api/2.0/sql/statements",
            headers=_auth_headers(),
            json=body,
            timeout=30.0,
        )
        if r.status_code >= 400:
            _TITLE_CACHE = {}
            return _TITLE_CACHE
        rows = (r.json().get("result") or {}).get("data_array") or []
        _TITLE_CACHE = {row[0]: row[1] for row in rows if row and row[0] and row[1]}
    except Exception:  # noqa: BLE001
        _TITLE_CACHE = {}
    return _TITLE_CACHE


def _enrich_titles(citations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    titles = _load_title_cache()
    for c in citations:
        rid = c.get("report_id")
        if rid and titles.get(rid):
            c["title"] = titles[rid]
    return citations


def _dedupe_citations(citations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """KA often returns multiple chunk-level citations for the same (report, page),
    one per retrieved chunk. Collapse them into a single citation card so the
    Sources panel lists each page once — and keep the longest snippet, since
    that's most useful for the in-PDF phrase matcher."""
    by_key: dict[tuple[str, Any], dict[str, Any]] = {}
    order: list[tuple[str, Any]] = []
    for c in citations:
        key = (c.get("report_id") or c.get("doc_uri") or "", c.get("page"))
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = dict(c)
            order.append(key)
            continue
        # Keep the longer snippet; prefer non-synthesized over synthesized.
        if c.get("synthesized") and not existing.get("synthesized"):
            continue
        if not c.get("synthesized") and existing.get("synthesized"):
            by_key[key] = dict(c)
            continue
        if len(c.get("snippet") or "") > len(existing.get("snippet") or ""):
            by_key[key] = dict(c)
    return [by_key[k] for k in order]


def _synthesize_citations_from_answer(answer: str) -> list[dict[str, Any]]:
    """If the model replies with an answer that mentions report_ids but returns
    no url_citation annotations (typical for the Supervisor → Genie path),
    surface those report_ids as lightweight citation cards pointing at the PDF."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for m in re.finditer(r"\b(\d{8}R\d+)\b", answer or ""):
        rid = m.group(1)
        if rid in seen:
            continue
        seen.add(rid)
        doc_uri = f"{VOLUME_ROOT.rstrip('/')}/{rid}.pdf"
        out.append({
            "doc_uri": doc_uri,
            "report_id": rid,
            "page": None,
            "section": None,
            "snippet": "",
            "volume_url": _volume_browser_url(doc_uri, None),
            "title": "",
            "synthesized": True,
        })
    return out


_KA_TOOL_HINTS = ("content_assistant", "knowledge", "research")

# Heuristic markers that the supervisor or its sub-agent hit a transient error
# and returned an apology instead of a real answer. If we detect this and the
# KA retry succeeds, we substitute the KA's answer.
_APOLOGY_MARKERS = (
    "technical error",
    "technical issue",
    "unable to retrieve",
    "encountered an error",
    "experienced a technical",
)


def _supervisor_used_ka(tools_called: list[str]) -> bool:
    return any(any(h in t.lower() for h in _KA_TOOL_HINTS) for t in tools_called)


def _looks_like_apology(answer: str) -> bool:
    low = (answer or "").lower()
    return any(m in low for m in _APOLOGY_MARKERS)


@app.post("/api/chat")
def chat(req: ChatRequest) -> dict[str, Any]:
    if req.route == "knowledge":
        endpoint = KA_ENDPOINT
    else:
        endpoint = SUPERVISOR_ENDPOINT

    payload = {"input": _build_input(req)}
    resp = _query_endpoint(endpoint, payload)
    parsed = _parse_response(resp)
    citations = parsed["citations"]
    answer = parsed["answer"]
    anchors = parsed.get("anchors") or []

    # Supervisor strips url_citation annotations when it relays the KA's answer,
    # and its sub-agent call can also flake transiently — returning an apology
    # in place of a real answer. In both cases, retrying the KA directly is
    # safe and produces the right content AND gives us inline anchor positions.
    #
    # We retry when EITHER (a) we can tell from tool names that the Supervisor
    # used the KA, OR (b) we got no citations back on what looks like a content
    # question (answer references a report_id / has rich numeric content). The
    # second clause covers Supervisor builds that don't expose the KA tool name
    # in `tools_called` — we still want inline-citation grounding in those.
    looks_like_content = bool(
        re.search(r"\b\d{8}R\d+\b", answer or "")
        or re.search(r"\d{1,4}(?:\.\d+)?\s*%", answer or "")
    )
    should_retry = (
        endpoint == SUPERVISOR_ENDPOINT
        and (not citations or not anchors or _looks_like_apology(answer))
        and (_supervisor_used_ka(parsed.get("tools_called") or []) or looks_like_content)
    )
    if should_retry:
        try:
            ka_resp = _query_endpoint(KA_ENDPOINT, {"input": _build_input(req)})
            ka_parsed = _parse_response(ka_resp)
            if ka_parsed["citations"]:
                # Replace the whole parse result with KA's: its answer has the
                # same content but ships with anchor offsets we can use to
                # inject inline [N] markers.
                citations = ka_parsed["citations"]
                if ka_parsed["answer"]:
                    answer = ka_parsed["answer"]
                    anchors = ka_parsed.get("anchors") or []
        except HTTPException:
            pass

    if not citations:
        citations = _synthesize_citations_from_answer(answer)

    # Remember the pre-dedup citation keys (they're the ones in `anchors`),
    # then build a key → post-dedup-index map so inline [N] markers point
    # at the right final citation.
    pre_keys = [
        (c.get("doc_uri") or "", c.get("page"), (c.get("snippet") or "")[:80])
        for c in citations
    ]
    citations = _dedupe_citations(citations)
    citations = _enrich_titles(citations)
    final_keys = {
        (c.get("doc_uri") or "", c.get("page"), (c.get("snippet") or "")[:80]): i
        for i, c in enumerate(citations)
    }
    # Some pre-dedup citations collapse into a single post-dedup citation; map
    # their keys to the same final index by matching on (doc_uri, page).
    key_to_idx: dict[tuple, int] = {}
    for k in pre_keys:
        if k in final_keys:
            key_to_idx[k] = final_keys[k]
            continue
        # Find any final citation with the same (doc_uri, page).
        for fk, fi in final_keys.items():
            if fk[0] == k[0] and fk[1] == k[1]:
                key_to_idx[k] = fi
                break

    before = answer
    answer = _inject_inline_markers(answer, anchors, citations, key_to_idx)
    # If the anchor-based injection didn't produce any [N] markers (either no
    # anchors came back, or all of them failed to map to a final citation),
    # fall back to paragraph-level token-overlap matching. This is how the
    # Supervisor path usually works, since it strips url_citation annotations.
    if answer == before and not re.search(r"\[\d+\]", answer):
        answer = _heuristic_inject(answer, citations)

    return {"answer": answer, "citations": citations, "endpoint": endpoint}


@app.get("/api/metadata/latest")
def latest() -> dict[str, Any]:
    sql = """
    WITH r AS (
      SELECT report_id, title, report_series, primary_author, team, asset_class,
             region, publication_date, summary, volume_path,
             ROW_NUMBER() OVER (PARTITION BY team ORDER BY publication_timestamp DESC) AS rn
      FROM research_assistant_demo.default.report_metadata
    )
    SELECT report_id, title, report_series, primary_author, team, asset_class,
           region, publication_date, summary, volume_path
    FROM r WHERE rn = 1
    ORDER BY publication_date DESC
    """
    url = f"{HOST}/api/2.0/sql/statements"
    body = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"}
    r = httpx.post(url, headers=_auth_headers(), json=body, timeout=60.0)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text[:1000])
    d = r.json()
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = d.get("result", {}).get("data_array", []) or []
    return {"columns": cols, "rows": rows}


@app.get("/api/pdf")
def pdf(path: str = Query(..., description="UC volume path like /Volumes/cat/sch/vol/file.pdf")) -> StreamingResponse:
    """Stream a PDF from a Unity Catalog volume.

    Restricted to the configured VOLUME_ROOT prefix so the endpoint can't be
    used as a general file-read proxy.
    """
    normalized = path.strip()
    root = VOLUME_ROOT.rstrip("/")
    if not normalized.startswith(root + "/") or ".." in normalized:
        raise HTTPException(status_code=400, detail="path outside of allowed volume root")
    try:
        resp = w.files.download(normalized)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"file not found: {e}") from e

    def _iter() -> Any:
        stream = resp.contents
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                break
            yield chunk

    filename = normalized.rsplit("/", 1)[-1]
    return StreamingResponse(
        _iter(),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=300",
        },
    )


# ---------------------------------------------------------------------------
# Server-side PDF page rendering + highlight overlay.
#
# Rationale: the previous approach tried to use PDF.js's find controller on
# the client to position highlights inside the rendered text layer. That path
# has a lot of moving parts (CDN-loaded worker, module timing, text-layer
# reassembly, ligature/whitespace quirks), and repeated attempts couldn't make
# the highlight land reliably on numeric phrases. Rendering the page to a PNG
# with the highlight drawn on in pymupdf sidesteps all of that — we compute
# the word bounding boxes from the PDF's text layer once, rasterize the page
# at a chosen DPI, and return a single image the client just displays.
# ---------------------------------------------------------------------------

# In-memory cache of {path: (expires_at_epoch, pdf_bytes)}. Reports are ~MBs
# each and rarely change during a session, so a tiny bounded cache keeps the
# per-page render fast (no re-download) without ever needing eviction logic
# beyond TTL. Max entries keeps total memory bounded.
_PDF_BYTES_CACHE: dict[str, tuple[float, bytes]] = {}
_PDF_CACHE_TTL = 30 * 60  # 30 minutes
_PDF_CACHE_MAX_ENTRIES = 16


def _get_pdf_bytes(path: str) -> bytes:
    now = time.time()
    cached = _PDF_BYTES_CACHE.get(path)
    if cached and cached[0] > now:
        return cached[1]
    # Expire stale entries lazily.
    for k, (exp, _) in list(_PDF_BYTES_CACHE.items()):
        if exp <= now:
            _PDF_BYTES_CACHE.pop(k, None)
    resp = w.files.download(path)
    buf = io.BytesIO()
    stream = resp.contents
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        buf.write(chunk)
    data = buf.getvalue()
    if len(_PDF_BYTES_CACHE) >= _PDF_CACHE_MAX_ENTRIES:
        # Evict the oldest.
        oldest_key = min(_PDF_BYTES_CACHE, key=lambda k: _PDF_BYTES_CACHE[k][0])
        _PDF_BYTES_CACHE.pop(oldest_key, None)
    _PDF_BYTES_CACHE[path] = (now + _PDF_CACHE_TTL, data)
    return data


_SNIPPET_STOPWORDS_FOR_SEARCH = {
    "the", "and", "for", "from", "with", "that", "this", "have", "are", "was",
    "were", "been", "will", "would", "could", "should", "may", "might",
}


def _candidate_phrases(snippet: str) -> list[str]:
    """Return a prioritized list of phrases to search for on the PDF page.

    PDF text layers often split words and reflow whitespace, so a
    verbatim snippet rarely matches. We try, in order:
      1. numeric-dense windows of 4-8 words (most likely to be distinctive
         AND quotable verbatim — forecasts, percentages, dollar amounts),
      2. the full cleaned snippet trimmed to ~12 words,
      3. the first meaningful 4-word window.
    """
    clean = re.sub(r"<[^>]+>", " ", snippet or "")
    clean = re.sub(r"\s+", " ", clean).strip()
    if not clean:
        return []
    words = clean.split(" ")
    seen: set[str] = set()
    out: list[str] = []

    def _add(s: str) -> None:
        s = s.strip(" .,:;!?\"'()")
        if not s or s in seen:
            return
        seen.add(s)
        out.append(s)

    has_num = re.compile(r"\d")
    # Numeric-dense windows, prefer longer then shorter.
    for size in (8, 7, 6, 5, 4):
        if size > len(words):
            continue
        ranked: list[tuple[int, str]] = []
        for i in range(len(words) - size + 1):
            win = words[i : i + size]
            score = sum(1 for w in win if has_num.search(w))
            if score >= 1:
                ranked.append((score, " ".join(win)))
        ranked.sort(key=lambda x: -x[0])
        for _, phrase in ranked[:3]:
            _add(phrase)
    # Full-ish snippet.
    _add(" ".join(words[:12]))
    # Fallback meaningful prefix.
    meaningful = [w for w in words if w.lower() not in _SNIPPET_STOPWORDS_FOR_SEARCH]
    if len(meaningful) >= 4:
        _add(" ".join(meaningful[:4]))
    return out


def _find_highlight_rects(page: "fitz.Page", snippet: str) -> list[tuple[float, float, float, float]]:
    """Try successively less-strict phrases from the snippet until something
    matches on the page. Returns bounding rects in PAGE coordinates (not
    pixel) — caller applies the render scale."""
    if not snippet:
        return []
    for phrase in _candidate_phrases(snippet):
        quads = page.search_for(phrase, quads=True)
        if quads:
            # Merge each quad's bbox into a rect.
            rects: list[tuple[float, float, float, float]] = []
            for q in quads:
                r = q.rect
                rects.append((r.x0, r.y0, r.x1, r.y1))
            return rects
    return []


def _merge_rects_by_line(rects: list[tuple[float, float, float, float]], y_tolerance: float = 3.0) -> list[tuple[float, float, float, float]]:
    """Merge rects that share the same visual line into a single box, so a
    phrase spanning multiple words renders as one continuous highlight rather
    than a bunch of tiny per-word boxes with gaps."""
    if not rects:
        return []
    sorted_rects = sorted(rects, key=lambda r: (r[1], r[0]))
    merged: list[list[float]] = []
    for x0, y0, x1, y1 in sorted_rects:
        if merged:
            mx0, my0, mx1, my1 = merged[-1]
            if abs(((y0 + y1) / 2) - ((my0 + my1) / 2)) <= y_tolerance:
                merged[-1] = [min(mx0, x0), min(my0, y0), max(mx1, x1), max(my1, y1)]
                continue
        merged.append([x0, y0, x1, y1])
    return [tuple(r) for r in merged]


@app.get("/api/pdf-render")
def pdf_render(
    path: str = Query(...),
    page: int = Query(1, ge=1),
    snippet: str = Query("", description="Text to highlight on the page"),
    scale: float = Query(1.6, ge=0.5, le=3.5, description="Render scale (DPI multiplier)"),
) -> Response:
    """Return a PNG of the PDF page with the snippet highlighted server-side.

    Response headers carry sidecar metadata the client needs:
      X-Pdf-Num-Pages, X-Pdf-Title, X-Highlight-Count.
    """
    normalized = path.strip()
    root = VOLUME_ROOT.rstrip("/")
    if not normalized.startswith(root + "/") or ".." in normalized:
        raise HTTPException(status_code=400, detail="path outside allowed volume root")

    try:
        pdf_bytes = _get_pdf_bytes(normalized)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"file not found: {e}") from e

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to open pdf: {e}") from e

    try:
        num_pages = doc.page_count
        target_page = max(1, min(page, num_pages))
        pg = doc.load_page(target_page - 1)
        rects = _find_highlight_rects(pg, snippet)
        merged = _merge_rects_by_line(rects)
        # Draw the highlight ON the page before rendering: this composites
        # cleanly under the rendered glyphs because add_highlight_annot places
        # the annotation in the page's blend layer.
        for r in merged:
            rect = fitz.Rect(*r)
            annot = pg.add_highlight_annot(rect)
            annot.set_colors(stroke=(1.0, 0.82, 0.20))  # warm amber
            annot.set_opacity(0.40)
            annot.update()
        mat = fitz.Matrix(scale, scale)
        pix = pg.get_pixmap(matrix=mat, alpha=False)
        png_bytes = pix.tobytes("png")
        headers = {
            "X-Pdf-Num-Pages": str(num_pages),
            "X-Highlight-Count": str(len(merged)),
            "Cache-Control": "private, max-age=300",
        }
        return Response(content=png_bytes, media_type="image/png", headers=headers)
    finally:
        doc.close()


@app.get("/api/pdf-info")
def pdf_info(path: str = Query(...)) -> dict[str, Any]:
    """Lightweight metadata: page count, so the client knows how many pages to
    allow paging through."""
    normalized = path.strip()
    root = VOLUME_ROOT.rstrip("/")
    if not normalized.startswith(root + "/") or ".." in normalized:
        raise HTTPException(status_code=400, detail="path outside allowed volume root")
    try:
        pdf_bytes = _get_pdf_bytes(normalized)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"file not found: {e}") from e
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        return {"num_pages": doc.page_count}
    finally:
        try:
            doc.close()
        except Exception:  # noqa: BLE001
            pass


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root() -> FileResponse:
    return FileResponse("static/index.html")
