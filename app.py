import os
import re
import urllib.parse
from typing import Any, Optional

import httpx
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

SUPERVISOR_ENDPOINT = os.environ.get("SUPERVISOR_ENDPOINT", "mas-d45b0de3-endpoint")
KA_ENDPOINT = os.environ.get("KA_ENDPOINT", "ka-77835fba-endpoint")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f13a85f3dd12ba954cc350e5092f74")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "8baced1ff014912d")

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
    parts = (doc_path or "").lstrip("/").split("/")
    if len(parts) >= 5 and parts[0] == "Volumes":
        catalog, schema, volume = parts[1], parts[2], parts[3]
        rest = "/".join(parts[4:])
        url = f"{HOST}/explore/data/volumes/{catalog}/{schema}/{volume}/{rest}"
        if page:
            url += f"#page={page}"
        return url
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
    """Parse the responses-API shape used by Agent Bricks serving endpoints."""
    final_text_parts: list[str] = []
    intermediate_text: list[str] = []
    citations: list[dict[str, Any]] = []
    seen = set()

    output = resp.get("output") or []
    # Track indices of message-type items to find the final one
    message_items = [(i, item) for i, item in enumerate(output) if isinstance(item, dict) and item.get("type") == "message"]

    for idx, item in enumerate(output):
        if not isinstance(item, dict):
            continue
        itype = item.get("type")
        if itype == "message":
            is_final = (idx == message_items[-1][0]) if message_items else False
            content = item.get("content") or []
            text_pieces = []
            for c in content:
                if not isinstance(c, dict):
                    continue
                if c.get("type") in ("output_text", "text"):
                    t = c.get("text") or ""
                    if _looks_internal(t):
                        continue
                    text_pieces.append(t)
                    for ann in c.get("annotations") or []:
                        if not isinstance(ann, dict):
                            continue
                        if ann.get("type") == "url_citation":
                            parsed = _parse_annotation(ann.get("url") or "")
                            if not parsed["doc_uri"] and not parsed["snippet"]:
                                continue
                            key = (parsed["doc_uri"], parsed["page"], parsed["snippet"][:80])
                            if key in seen:
                                continue
                            seen.add(key)
                            title = ann.get("title") or ""
                            citations.append(
                                {
                                    "doc_uri": parsed["doc_uri"],
                                    "report_id": parsed["report_id"],
                                    "page": parsed["page"],
                                    "section": None,
                                    "snippet": parsed["snippet"],
                                    "volume_url": _volume_browser_url(parsed["doc_uri"], parsed["page"]),
                                    "title": title,
                                }
                            )
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

    return {"answer": final_text, "citations": citations}


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "supervisor": SUPERVISOR_ENDPOINT,
        "ka": KA_ENDPOINT,
        "genie_space": GENIE_SPACE_ID,
        "host": HOST,
    }


@app.post("/api/chat")
def chat(req: ChatRequest) -> dict[str, Any]:
    if req.route == "knowledge":
        endpoint = KA_ENDPOINT
    else:
        endpoint = SUPERVISOR_ENDPOINT

    payload = {"input": _build_input(req)}
    resp = _query_endpoint(endpoint, payload)
    parsed = _parse_response(resp)
    return {"answer": parsed["answer"], "citations": parsed["citations"], "endpoint": endpoint}


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


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root() -> FileResponse:
    return FileResponse("static/index.html")
