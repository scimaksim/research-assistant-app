# Research Assistant

A redeployable Databricks demo that lets analysts chat with a library of research PDFs — metadata questions go to **Genie**, content questions go to an Agent Bricks **Knowledge Assistant**, and a **Multi-Agent Supervisor** routes between them. Citations open the exact PDF page in the workspace.

```
 PDF volume ──┐                     ┌──> Knowledge Assistant ──┐
              ├──> ai_extract ──>   │                           ├──> Supervisor ──> FastAPI App
 metadata  ──┘     report_metadata ─┴──> Genie space ──────────┘
```

## What's in the repo

| Path | Purpose |
|------|---------|
| `app.py` / `app.yaml` | FastAPI app that calls the Supervisor endpoint and parses citations |
| `static/` | Vanilla-JS chat UI with citation panel |
| `databricks.yml` | Databricks Asset Bundle root — declares the app, jobs, variables |
| `resources/*.yml` | Bundle resource definitions (app + jobs) |
| `bundle/setup_uc.py` | Creates catalog/schema/volume/table |
| `bundle/extract_metadata.py` | `ai_extract` → `report_metadata` table (idempotent MERGE) |
| `bundle/create_agent_bricks.py` | Creates Genie + Knowledge Assistant + Supervisor |
| `bundle/resync_ka.py` | Re-indexes the KA after PDFs change |
| `bundle/add_pdf.py` | One-liner to upload a PDF and kick off re-index |
| `eval/eval_retrieval.py` | Retrieval quality eval harness |

## Redeploy into a new workspace

```bash
# 1. Point the Databricks CLI at the target workspace
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile=target

# 2. Edit databricks.yml (or override at deploy time):
#    - workspace.host
#    - variables.catalog, schema, volume, metadata_table
#    - variables.warehouse_id (serverless SQL warehouse you have CAN_USE on)

# 3. Deploy catalog/schema/volume/table + jobs
databricks bundle deploy -t dev --profile=target

# 4. Drop your PDFs into the volume
databricks fs cp ./reports/*.pdf \
  dbfs:/Volumes/CATALOG/SCHEMA/VOLUME/ --profile=target

# 5. Run the bootstrap job — extracts metadata and creates Genie/KA/Supervisor.
#    It writes endpoint names + IDs to /Workspace/.bundle/research-assistant/outputs.json
databricks bundle run bootstrap -t dev --profile=target

# 6. Plug those three values back into databricks.yml variables, then redeploy
#    the app so app.yaml resource bindings pick them up:
#      variables.supervisor_endpoint
#      variables.ka_endpoint
#      variables.genie_space_id
databricks bundle deploy -t dev --profile=target
```

## Add a PDF later

```bash
python bundle/add_pdf.py \
  --catalog CATALOG --schema SCHEMA --volume VOLUME \
  ./path/to/new_report.pdf
```

This uploads to the volume and kicks off the `research-assistant-reindex` job, which re-extracts metadata and re-syncs the KA vector index.

## Evaluate retrieval quality

**UI path (recommended for iteration):** open the Vector Search index that backs the KA in the Databricks UI and click **Evaluate search quality**. Databricks samples docs, LLM-generates queries, and reports DCG@10, NDCG, Recall/Precision, MRR, MAP. See [Retrieval quality eval](https://docs.databricks.com/aws/en/vector-search/retrieval-quality-eval).

**Programmatic path (runs in CI):**

```bash
# Edit eval/questions.yml with golden-path questions + expected citations/substrings
python eval/eval_retrieval.py \
  --ka-endpoint ka-XXXX-endpoint \
  --supervisor-endpoint mas-XXXX-endpoint
```

Aggregates: `avg_answer_coverage`, `avg_citation_coverage`, `any_citation_rate`. Logs to MLflow if a tracking server is configured.

## Local dev

```bash
pip install -r requirements.txt
export DATABRICKS_CONFIG_PROFILE=target
export SUPERVISOR_ENDPOINT=mas-XXXX-endpoint
export KA_ENDPOINT=ka-XXXX-endpoint
export GENIE_SPACE_ID=01fXXXX
export DATABRICKS_WAREHOUSE_ID=XXXX
uvicorn app:app --reload
```
