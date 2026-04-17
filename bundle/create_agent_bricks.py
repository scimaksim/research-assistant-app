"""Create (or look up) Genie space, Knowledge Assistant, Multi-Agent Supervisor.

Writes the resulting IDs / endpoint names to stdout AND to the bundle workspace
file `/Workspace/.bundle/research-assistant/outputs.json` so subsequent runs
(and the app) can read them.

Idempotent: finds existing resources by display_name before creating.
"""
import argparse
import json
import os
import sys
import time
from typing import Any, Optional

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.agents import (
    KnowledgeAssistant,
    KnowledgeSource,
    FilesSpec,
)


DISPLAY_NAME_KA = "Research Assistant Knowledge"
DISPLAY_NAME_GENIE = "Research Metadata Genie"
DISPLAY_NAME_SUPERVISOR = "Research Assistant Supervisor"


def rest(w: WorkspaceClient, method: str, path: str, **kw) -> Any:
    host = w.config.host.rstrip("/")
    auth = w.config.authenticate()
    r = httpx.request(method, f"{host}{path}", headers=auth, timeout=120.0, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.text else {}


def ensure_genie_space(w: WorkspaceClient, catalog: str, schema: str, table: str, warehouse_id: str) -> str:
    """POST /api/2.0/data-rooms — returns space id."""
    existing = rest(w, "GET", "/api/2.0/data-rooms")
    for sp in existing.get("data_rooms", []) or []:
        if sp.get("display_name") == DISPLAY_NAME_GENIE:
            return sp["id"]
    body = {
        "display_name": DISPLAY_NAME_GENIE,
        "description": "Structured metadata about research reports (title, author, team, publication date).",
        "warehouse_id": warehouse_id,
        "table_identifiers": [f"{catalog}.{schema}.{table}"],
    }
    created = rest(w, "POST", "/api/2.0/data-rooms", json=body)
    return created["id"]


def ensure_knowledge_assistant(w: WorkspaceClient, catalog: str, schema: str, volume: str) -> tuple[str, str]:
    """Returns (ka_id, endpoint_name)."""
    for ka in w.knowledge_assistants.list_knowledge_assistants():
        if ka.display_name == DISPLAY_NAME_KA:
            return ka.id, ka.endpoint_name
    ka = w.knowledge_assistants.create_knowledge_assistant(
        knowledge_assistant=KnowledgeAssistant(
            display_name=DISPLAY_NAME_KA,
            description="Analyst research PDFs: macro, credit, commodities, ETFs, rates.",
            instructions=(
                "Answer questions from sell-side research PDFs. Always cite the report "
                "by report_id and page. If the retrieval returns no relevant passages, "
                "say so explicitly rather than guessing."
            ),
        )
    )
    vol_path = f"/Volumes/{catalog}/{schema}/{volume}"
    w.knowledge_assistants.create_knowledge_source(
        parent=f"knowledge-assistants/{ka.id}",
        knowledge_source=KnowledgeSource(
            display_name="Research PDFs",
            description="Analyst research PDFs.",
            source_type="files",
            files=FilesSpec(path=vol_path),
        ),
    )
    # Poll until endpoint is ready
    for _ in range(60):
        k = w.knowledge_assistants.get_knowledge_assistant(name=f"knowledge-assistants/{ka.id}")
        if k.endpoint_name:
            return k.id, k.endpoint_name
        time.sleep(10)
    raise RuntimeError("Knowledge Assistant endpoint did not become ready")


def ensure_supervisor(w: WorkspaceClient, genie_id: str, ka_id: str, ka_endpoint: str) -> tuple[str, str]:
    """Create Multi-Agent Supervisor via REST /api/2.1/supervisor-agents.
    Returns (supervisor_id, endpoint_name).
    """
    existing = rest(w, "GET", "/api/2.1/supervisor-agents")
    for s in existing.get("supervisor_agents", []) or []:
        if s.get("display_name") == DISPLAY_NAME_SUPERVISOR:
            return s["id"], s.get("endpoint_name", "")
    body = {
        "display_name": DISPLAY_NAME_SUPERVISOR,
        "description": "Routes questions to Genie (metadata) or Knowledge Assistant (content).",
        "instructions": (
            "You are a research-library router. Route content questions "
            "(forecasts, sector views, analysis summaries) to the knowledge assistant. "
            "Route metadata questions (latest, newest, author, team) to Genie."
        ),
    }
    created = rest(w, "POST", "/api/2.1/supervisor-agents", json=body)
    sup_id = created["id"]
    # Attach Genie tool
    rest(
        w,
        "POST",
        f"/api/2.1/supervisor-agents/{sup_id}/tools?tool_id=Metadata_Genie",
        json={
            "tool_type": "genie",
            "description": "Structured metadata queries over report_metadata.",
            "genie": {"space_id": genie_id},
        },
    )
    # Attach KA tool
    rest(
        w,
        "POST",
        f"/api/2.1/supervisor-agents/{sup_id}/tools?tool_id=Content_Assistant",
        json={
            "tool_type": "knowledge_assistant",
            "description": "Semantic search over research PDFs with citations.",
            "knowledge_assistant": {
                "serving_endpoint_name": ka_endpoint,
                "knowledge_assistant_id": ka_id,
            },
        },
    )
    # Deploy
    rest(w, "POST", f"/api/2.1/supervisor-agents/{sup_id}/deploy")
    # Poll
    for _ in range(60):
        s = rest(w, "GET", f"/api/2.1/supervisor-agents/{sup_id}")
        if s.get("endpoint_name"):
            return sup_id, s["endpoint_name"]
        time.sleep(10)
    raise RuntimeError("Supervisor endpoint did not become ready")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--metadata-table", required=True)
    args = p.parse_args()

    w = WorkspaceClient()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        # Fallback: pick first running warehouse
        for wh in w.warehouses.list():
            if wh.state and wh.state.value in ("RUNNING", "STARTING"):
                warehouse_id = wh.id
                break

    print("[bootstrap] creating Genie space ...")
    genie_id = ensure_genie_space(w, args.catalog, args.schema, args.metadata_table, warehouse_id)
    print(f"[bootstrap]   genie_space_id = {genie_id}")

    print("[bootstrap] creating Knowledge Assistant ...")
    ka_id, ka_endpoint = ensure_knowledge_assistant(w, args.catalog, args.schema, args.volume)
    print(f"[bootstrap]   ka_endpoint = {ka_endpoint}")

    print("[bootstrap] creating Multi-Agent Supervisor ...")
    sup_id, sup_endpoint = ensure_supervisor(w, genie_id, ka_id, ka_endpoint)
    print(f"[bootstrap]   supervisor_endpoint = {sup_endpoint}")

    outputs = {
        "genie_space_id": genie_id,
        "ka_endpoint": ka_endpoint,
        "ka_id": ka_id,
        "supervisor_endpoint": sup_endpoint,
        "supervisor_id": sup_id,
    }
    print(json.dumps({"outputs": outputs}))

    # Persist outputs so the app + other tooling can read them
    try:
        from databricks.sdk.service.workspace import ImportFormat
        path = "/Workspace/.bundle/research-assistant/outputs.json"
        w.workspace.mkdirs("/Workspace/.bundle/research-assistant")
        w.workspace.upload(
            path,
            json.dumps(outputs, indent=2).encode(),
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        print(f"[bootstrap] wrote {path}")
    except Exception as e:
        print(f"[bootstrap] (non-fatal) could not write outputs.json: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[bootstrap] FAILED: {e}", file=sys.stderr)
        raise
