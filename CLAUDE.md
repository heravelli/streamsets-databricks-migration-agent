# CLAUDE.md — StreamSets → Databricks Migration Agent

## Project Purpose

AI-powered agent that migrates **2000 StreamSets Data Collector pipelines** across **20 teams** to Databricks. Source pipelines are JSON/YAML exports. The agent classifies each pipeline and generates Databricks Python code (DLT, Job, or Notebook). Human reviewers approve each migration via a FastAPI portal.

---

## Git Workflow

- **Always create a feature branch** before making any code changes — never commit directly to `main`.
- Branch naming: `feature/<short-description>` (e.g. `feature/add-snowflake-origin`)
- Open a PR from the branch; do not push directly to `main`.
- Only switch to `main` to pull the latest after a PR is merged.

```bash
git checkout main && git pull
git checkout -b feature/<description>
# ... make changes, commit ...
git push -u origin feature/<description>
```

---

## Package Manager

Always use **`uv`** — never call `pip` directly.

```bash
uv add <package>               # add dependency
uv run migrate <command>       # run CLI
uv sync --no-dev               # install in Docker
```

---

## CLI Commands

| Command | Purpose |
|---|---|
| `uv run migrate ingest <dir> --team <name>` | Parse exports, register in state |
| `uv run migrate run --team <name> --concurrency 5` | Run agent on a team's pending pipelines |
| `uv run migrate run --pipeline-id <id>` | Run agent on a single pipeline |
| `uv run migrate run --all --concurrency 10` | Process all 2000 pipelines |
| `uv run migrate serve [--port 8080]` | Start FastAPI review portal |
| `uv run migrate status [--team <name>]` | Print Rich progress table |
| `uv run migrate export <dir> [--format zip]` | Bundle approved artifacts |

---

## Architecture

```
streamsets_databrickst_migration_agent/
├── agent/
│   ├── migration_agent.py       # Main agentic loop (tool-use orchestrator, max 20 iterations)
│   ├── client_factory.py        # Returns ClaudeClient or GatewayClient based on env var
│   ├── claude_client.py         # Direct Anthropic SDK wrapper with retry (DO NOT overwrite with gateway code)
│   ├── gateway_client.py        # Enterprise AI gateway client (OpenAI-compatible adapter)
│   ├── context_builder.py       # Builds per-pipeline Markdown prompt (~4000 token target)
│   ├── tools/
│   │   ├── classify_pipeline.py     # Tool: recommend DLT/Job/Notebook
│   │   ├── lookup_stage_mapping.py  # Tool: catalog lookup per stage
│   │   └── emit_migration_result.py # Tool: final artifact emission (called once at end)
│   └── prompts/
│       └── system_prompt.md     # Decision tree + code generation rules
├── catalog/
│   ├── stage_catalog.py         # YAML loader + lookup (suffix-matches versioned FQCNs)
│   └── stages/
│       ├── origins.yaml         # Source stage mappings
│       ├── processors.yaml      # Processor stage mappings
│       ├── destinations.yaml    # Destination stage mappings
│       └── executors.yaml       # Executor stage mappings
├── models/
│   ├── streamsets.py            # StreamSetsPipeline, StageDefinition, StageType enum
│   ├── stage_mapping.py         # StageMapping, ConfidenceLevel, DatabricksTargetType
│   └── migration.py             # PipelineRecord, MigrationState, PipelineStatus, ReviewDecision
├── parser/
│   ├── pipeline_parser.py       # JSON/YAML → StreamSetsPipeline; scan_directory()
│   └── pipeline_classifier.py  # Heuristics for DLT vs Job vs Notebook
├── state/
│   ├── state_manager.py         # File-based state with portalocker + atomic rename
│   └── progress_tracker.py      # Team-level progress aggregation
├── review/
│   ├── app.py                   # FastAPI factory create_app()
│   ├── routers/
│   │   ├── teams.py             # GET /teams/progress — dashboard
│   │   └── pipelines.py        # Review queue, decisions, diff view
│   └── templates/               # Jinja2 HTML (dashboard, pipeline_review, diff)
├── cli/
│   ├── main.py                  # Typer entry point
│   └── commands/                # ingest, migrate, serve, status, export
├── config/
│   └── settings.py              # Pydantic BaseSettings — all config from env vars
├── data/
│   ├── pipelines/[team]/*.json  # Input StreamSets exports
│   └── state/migration_state.json
└── output/[team]/[pipeline_id]/ # Generated artifacts
    ├── pipeline.py
    └── migration_metadata.json
```

---

## AI Client Selection

Two clients implement the **same async interface** — `migration_agent.py` never knows which it has.

| `AGENT_CLIENT_TYPE` | Client | API |
|---|---|---|
| `anthropic` (default) | `claude_client.py` | Anthropic SDK directly |
| `gateway` | `gateway_client.py` | OpenAI-compatible HTTP (enterprise gateway) |

**Critical**: `claude_client.py` is the direct Anthropic implementation. `gateway_client.py` is the adapter. Never merge them or overwrite one with the other. The factory in `client_factory.py` routes between them.

Gateway adapter converts: Anthropic tool schemas → OpenAI function calling, Anthropic messages → OpenAI chat format, OpenAI response → Anthropic-compatible wrapper objects.

---

## Key Conventions

- **File locking**: Use `portalocker` (not `fcntl`) — works on both Mac (dev) and Linux AKS containers.
- **Config**: All settings via `config/settings.py` (Pydantic BaseSettings). Read from `.env`. No hardcoded paths.
- **Atomic state writes**: Write to temp file → `portalocker` exclusive lock → rename. Never write directly.
- **Concurrency**: `asyncio.Semaphore(n)` around Claude API calls. Default `AGENT_CONCURRENCY=3`.
- **Retry**: 3 attempts with exponential backoff (5s/10s/20s for rate limits, 2s/4s for 5xx).
- **Model**: `claude-sonnet-4-6` (configurable via `ANTHROPIC_MODEL` or `AI_GATEWAY_MODEL`).

---

## Agent Decision Tree

```
SELECT DLT when:      CDC/streaming origin, Kafka, continuous execution mode
SELECT JOB when:      Batch/cron, stateless ETL, no streaming origins
SELECT NOTEBOOK when: >15 stages, Groovy/Jython custom code, 3+ output lanes, >2 LOW-confidence stages
```

Pipeline status lifecycle:
```
pending → migrating → in_review → approved
                   ↘ changes_requested → re_migrating → in_review
                   ↘ escalated | rejected | failed
```

---

## Stage Catalog

**78 stages** (authoritative list in `streamsets_stages.md`) split across 4 YAML files in `catalog/stages/`:
- `origins.yaml` — 20 origins
- `processors.yaml` — 31 processors
- `destinations.yaml` — 18 destinations (includes BigQueryDTarget + GCSTarget misclassified as executors in streamsets_stages.md)
- `executors.yaml` — 8 executors

FQCN format: underscores in `streamsets_stages.md` IDs map to dots in YAML keys
(e.g. `com_streamsets_pipeline_stage_origin_multikafka_MultiKafkaDSource` → `com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource`)

Each entry:

```yaml
- streamsets_stage: "com.streamsets.pipeline.stage.origin.kafka.KafkaDSource"
  streamsets_label: "Kafka Consumer"
  databricks_equivalent: "dlt_streaming_table"
  confidence: "exact"           # exact | high | medium | low | unsupported
  code_template: |              # Jinja2 template
    @dlt.table(name="{{ table_name }}")
    def {{ table_name }}():
        return spark.readStream.format("kafka")...
  config_mapping:
    kafkaConfigBean.metadataBrokerList: "bootstrap_servers"
  requires_manual_review: false
```

To add a new stage: add an entry to the appropriate YAML file. `stage_catalog.py` auto-loads on startup and suffix-matches versioned class names.

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_CLIENT_TYPE` | `anthropic` | `anthropic` or `gateway` |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Model for direct Anthropic client |
| `AI_GATEWAY_URL` | — | Enterprise gateway endpoint |
| `AI_GATEWAY_TOKEN` | — | Bearer token for gateway |
| `AI_GATEWAY_MODEL` | — | Model name as exposed by gateway |
| `DATA_DIR` | `data/pipelines` | Input pipeline exports |
| `OUTPUT_DIR` | `output` | Generated artifact output |
| `STATE_FILE` | `data/state/migration_state.json` | Migration state JSON |
| `CATALOG_DIR` | `catalog/stages` | Stage YAML files |
| `PROMPTS_DIR` | `agent/prompts` | System prompt location |
| `AGENT_MAX_TOKENS` | `8096` | Max tokens per LLM call |
| `AGENT_CONCURRENCY` | `3` | Parallel pipeline migrations |
| `AGENT_COMPACT_CONTEXT` | `false` | Set `true` for token-limited gateway models — reduces input prompt ~60% by omitting stage config details and topology |
| `REVIEW_HOST` | `0.0.0.0` | Portal bind address |
| `REVIEW_PORT` | `8000` | Portal port |

---

## Deployment

### Local Dev
```bash
docker-compose up          # mounts ./data:/data
uv run migrate serve       # or run directly
```

### AKS (Production)
- `DATA_DIR`, `OUTPUT_DIR`, `STATE_FILE` point to a **PersistentVolumeClaim** mounted at `/data`
- `ANTHROPIC_API_KEY` / `AI_GATEWAY_TOKEN` injected via Kubernetes Secret
- Review portal runs as always-on pod; `migrate run`/`migrate ingest` run as Kubernetes Jobs
- `/health` endpoint on FastAPI for liveness/readiness probes

---

## Human Review Portal

Start with `uv run migrate serve`. Runs on `:8000` by default.

- `GET /teams/progress` — dashboard: 20-team progress table with completion %
- `GET /pipelines?team=X&status=in_review` — reviewer queue
- `GET /pipelines/{id}/review` — side-by-side: original JSON + generated Python + agent reasoning
- `POST /pipelines/{id}/decision` — body: `{decision, feedback, reviewer_id}`
  - `approve` → artifact written to `output/`, status = APPROVED
  - `request_changes` → re-migration triggered with feedback injected into context
  - `escalate` → senior engineer queue
  - `reject` → terminal state
- `GET /pipelines/{id}/diff` — unified diff between current and previous version
