"""Add one or more PDFs to the volume and trigger a re-index.

Usage (local, authenticated via the Databricks CLI):
    python bundle/add_pdf.py --catalog research_assistant_demo \
        --schema default --volume research_pdfs \
        ./path/to/new_report.pdf ./another.pdf

After uploading, runs the `reindex` bundle job to refresh metadata and the KA
vector index. If you've not yet deployed the bundle, pass --skip-reindex and
trigger the job manually.
"""
import argparse
import os
import sys

from databricks.sdk import WorkspaceClient


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--job-name", default="research-assistant-reindex")
    p.add_argument("--skip-reindex", action="store_true")
    p.add_argument("files", nargs="+", help="Local PDF paths to upload")
    args = p.parse_args()

    w = WorkspaceClient()
    target = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    uploaded: list[str] = []
    for local in args.files:
        if not os.path.exists(local):
            print(f"[add_pdf] skip (missing): {local}", file=sys.stderr)
            continue
        if not local.lower().endswith(".pdf"):
            print(f"[add_pdf] skip (not a pdf): {local}", file=sys.stderr)
            continue
        fname = os.path.basename(local)
        remote = f"{target}/{fname}"
        with open(local, "rb") as f:
            w.files.upload(remote, f, overwrite=True)
        uploaded.append(remote)
        print(f"[add_pdf] uploaded -> {remote}")

    if not uploaded:
        print("[add_pdf] nothing to upload")
        return

    if args.skip_reindex:
        print("[add_pdf] --skip-reindex: not triggering reindex job")
        return

    # Trigger the reindex job
    jobs = list(w.jobs.list(name=args.job_name))
    if not jobs:
        print(
            f"[add_pdf] reindex job '{args.job_name}' not found — deploy the bundle first "
            f"or re-run with --skip-reindex.",
            file=sys.stderr,
        )
        sys.exit(2)
    run = w.jobs.run_now(job_id=jobs[0].job_id)
    print(f"[add_pdf] reindex job triggered: run_id={run.run_id}")


if __name__ == "__main__":
    main()
