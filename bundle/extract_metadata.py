"""Populate the metadata table from PDFs in the volume using ai_extract.

MERGE semantics — existing rows aren't duplicated, new PDFs are added, stale rows
(whose volume_path no longer exists) are deleted. Call this whenever a PDF is
added or removed.
"""
import argparse
import sys
import time

import httpx
from databricks.sdk import WorkspaceClient


PROMPT_FIELDS = (
    "report_id, title, report_series, primary_author, team, asset_class, "
    "region, publication_date, summary"
)


def run_sql(w: WorkspaceClient, wid: str, stmt: str) -> dict:
    host = w.config.host.rstrip("/")
    auth = w.config.authenticate()
    url = f"{host}/api/2.0/sql/statements"
    body = {"warehouse_id": wid, "statement": stmt, "wait_timeout": "50s"}
    r = httpx.post(url, headers=auth, json=body, timeout=240.0)
    r.raise_for_status()
    d = r.json()
    state = d.get("status", {}).get("state")
    sid = d.get("statement_id")
    while state in ("PENDING", "RUNNING"):
        time.sleep(3)
        rr = httpx.get(f"{url}/{sid}", headers=auth, timeout=60.0)
        rr.raise_for_status()
        d = rr.json()
        state = d.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({state}): {d}")
    return d


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--metadata-table", required=True)
    p.add_argument("--warehouse-id", required=True)
    args = p.parse_args()

    w = WorkspaceClient()
    wid = args.warehouse_id
    cat, sch, vol, tbl = args.catalog, args.schema, args.volume, args.metadata_table
    vol_path = f"/Volumes/{cat}/{sch}/{vol}"

    staging_view = f"{cat}.{sch}.{tbl}_staging_v"
    # ai_extract on each PDF to produce a structured row
    extract_sql = f"""
    CREATE OR REPLACE TEMP VIEW {tbl}_extracted AS
    SELECT
      path AS volume_path,
      ai_extract(
        ai_parse_document(content).text_content,
        ARRAY('{PROMPT_FIELDS.replace(", ", "','")}')
      ) AS fields
    FROM READ_FILES('{vol_path}/*.pdf', format => 'binaryFile')
    """

    # Normalize into the metadata table shape
    upsert_sql = f"""
    MERGE INTO {cat}.{sch}.{tbl} t
    USING (
      SELECT
        fields.report_id AS report_id,
        fields.title AS title,
        fields.report_series AS report_series,
        fields.primary_author AS primary_author,
        fields.team AS team,
        fields.asset_class AS asset_class,
        fields.region AS region,
        fields.publication_date AS publication_date,
        TRY_CAST(fields.publication_date AS TIMESTAMP) AS publication_timestamp,
        fields.summary AS summary,
        volume_path
      FROM {tbl}_extracted
      WHERE fields.report_id IS NOT NULL
    ) s
    ON t.volume_path = s.volume_path
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    print(f"[extract_metadata] volume={vol_path}")
    run_sql(w, wid, extract_sql)
    run_sql(w, wid, upsert_sql)
    # Remove rows whose PDFs are no longer in the volume
    prune_sql = f"""
    DELETE FROM {cat}.{sch}.{tbl}
    WHERE volume_path NOT IN (
      SELECT path FROM READ_FILES('{vol_path}/*.pdf', format => 'binaryFile')
    )
    """
    run_sql(w, wid, prune_sql)
    print("[extract_metadata] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[extract_metadata] FAILED: {e}", file=sys.stderr)
        raise
