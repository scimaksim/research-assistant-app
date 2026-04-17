"""Retrieval-quality eval for the Knowledge Assistant.

Two modes:

  1. UI eval (recommended for iterating in the console):
       Open the Vector Search index that backs the KA and click
       "Evaluate search quality" — it will sample docs and LLM-generate queries.
       See `--print-ui-link` to print the index URL.

  2. Python eval (this script):
       Runs a curated YAML eval set against either the KA serving endpoint
       directly or the Supervisor endpoint, and scores retrieval with simple
       needle-matching + coverage-at-k. Writes an MLflow run if tracking is
       configured, else prints a summary.

Eval set shape (eval/questions.yml):

  - question: "What was the 2025 gold forecast?"
    route: knowledge          # or supervisor
    expect_doc_ids: ["12809886R1"]   # report_ids expected in citations
    expect_substrings: ["$3,063"]     # expected text in the answer
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import yaml


def get_token() -> str:
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    r = subprocess.run(
        ["databricks", "auth", "token", f"--profile={profile}"],
        capture_output=True, text=True, check=True,
    )
    return json.loads(r.stdout)["access_token"]


def ask(host: str, endpoint: str, token: str, q: str) -> dict:
    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    body = {"input": [{"role": "user", "content": q}]}
    r = httpx.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=120.0,
    )
    r.raise_for_status()
    return r.json()


def extract(resp: dict) -> tuple[str, list[str]]:
    """Return (final_text, [report_ids_cited])."""
    text_parts: list[str] = []
    doc_ids: list[str] = []
    out = resp.get("output") or []
    msgs = [i for i in out if isinstance(i, dict) and i.get("type") == "message"]
    final = msgs[-1] if msgs else None
    if final:
        for c in final.get("content") or []:
            if c.get("type") in ("output_text", "text"):
                text_parts.append(c.get("text") or "")
                for a in c.get("annotations") or []:
                    u = a.get("url") or ""
                    import re
                    m = re.search(r"(\d+R\d+)\.pdf", u)
                    if m:
                        doc_ids.append(m.group(1))
    return ("".join(text_parts), doc_ids)


def score(answer: str, cited: list[str], expect_subs: list[str], expect_docs: list[str]) -> dict:
    ans_l = answer.lower()
    sub_hit = sum(1 for s in expect_subs if s.lower() in ans_l)
    doc_hit = sum(1 for d in expect_docs if d in cited)
    return {
        "answer_coverage": sub_hit / max(1, len(expect_subs)),
        "citation_coverage": doc_hit / max(1, len(expect_docs)),
        "any_citation": 1.0 if cited else 0.0,
        "cited_docs": cited,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--eval-set", default="eval/questions.yml")
    p.add_argument("--host", default=os.environ.get("DATABRICKS_HOST") or "https://e2-demo-field-eng.cloud.databricks.com")
    p.add_argument("--ka-endpoint", default=os.environ.get("KA_ENDPOINT", "ka-77835fba-endpoint"))
    p.add_argument("--supervisor-endpoint", default=os.environ.get("SUPERVISOR_ENDPOINT", "mas-d45b0de3-endpoint"))
    p.add_argument("--output", default="eval/latest_results.json")
    args = p.parse_args()

    token = get_token()
    cases = yaml.safe_load(Path(args.eval_set).read_text())

    results = []
    per_case_rows = []
    for case in cases:
        q = case["question"]
        route = case.get("route", "knowledge")
        endpoint = args.ka_endpoint if route == "knowledge" else args.supervisor_endpoint
        t0 = time.time()
        try:
            resp = ask(args.host, endpoint, token, q)
        except Exception as e:
            per_case_rows.append({"question": q, "error": str(e)})
            continue
        dt = time.time() - t0
        ans, cited = extract(resp)
        m = score(ans, cited, case.get("expect_substrings", []), case.get("expect_doc_ids", []))
        per_case_rows.append({
            "question": q,
            "route": route,
            "latency_s": round(dt, 2),
            "answer_snippet": ans[:180].replace("\n", " "),
            **m,
        })
        results.append(m)

    # Aggregate
    agg = {
        "n": len(results),
        "avg_answer_coverage": round(sum(r["answer_coverage"] for r in results) / max(1, len(results)), 3),
        "avg_citation_coverage": round(sum(r["citation_coverage"] for r in results) / max(1, len(results)), 3),
        "any_citation_rate": round(sum(r["any_citation"] for r in results) / max(1, len(results)), 3),
    }
    print(json.dumps({"aggregate": agg, "cases": per_case_rows}, indent=2))

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps({"aggregate": agg, "cases": per_case_rows}, indent=2))
    print(f"[eval] wrote {args.output}")

    # Optional: log to MLflow if available
    try:
        import mlflow
        mlflow.set_experiment("/Users/" + os.environ.get("USER", "eval") + "/research-assistant-eval")
        with mlflow.start_run(run_name=f"retrieval-{int(time.time())}"):
            mlflow.log_metrics(agg)
            mlflow.log_artifact(args.output)
        print("[eval] logged run to MLflow")
    except Exception as e:
        print(f"[eval] MLflow logging skipped: {e}")


if __name__ == "__main__":
    main()
