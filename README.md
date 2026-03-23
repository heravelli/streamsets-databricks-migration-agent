# StreamSets → Databricks Migration Agent

AI-powered agent to migrate StreamSets Data Collector pipelines to Databricks (Delta Live Tables, Jobs, or Notebooks) — with a human-in-the-loop review portal.

Built for scale: 2000 pipelines, 20 teams, 78 unique stages.

---

## How It Works

```
1. Ingest   → Parse StreamSets JSON/YAML exports, register in state
2. Migrate  → AI agent converts each pipeline to Databricks Python code
3. Review   → Humans approve, request changes, or escalate via web portal
4. Export   → Bundle approved artifacts for deployment
```

The agent auto-selects the best Databricks target per pipeline:
- **Delta Live Tables (DLT)** — streaming pipelines (Multi-topic Kafka, Azure Event Hubs)
- **Databricks Job** — batch/cron pipelines
- **Notebook** — complex pipelines with custom code (Groovy/Jython) or many stages

---

## Call Flow Diagrams

### System Overview

```mermaid
flowchart TD
    User(["👤 User / Team Member"])

    subgraph CLI ["CLI  (uv run migrate ...)"]
        Ingest["ingest\nParse JSON/YAML exports"]
        Run["run\nTrigger migration agent"]
        Serve["serve\nStart review portal"]
        Status["status\nProgress report"]
        Export["export\nBundle approved artifacts"]
    end

    subgraph Agent ["Migration Agent"]
        Factory["client_factory\nSelect AI client"]
        MigAgent["MigrationAgent\nAgentic loop"]
        CtxBuilder["context_builder\nBuild pipeline prompt"]

        subgraph Clients ["AI Clients"]
            Claude["ClaudeClient\nAnthropic SDK"]
            Gateway["GatewayClient\nEnterprise AI gateway"]
            OpenAI["GatewayClient\nOpenAI API"]
        end

        subgraph Tools ["Tools (no LLM)"]
            Classify["classify_pipeline"]
            Lookup["lookup_stage_mapping"]
            Emit["emit_migration_result"]
        end
    end

    subgraph Catalog ["Stage Catalog"]
        YAML["78 stages\norigins / processors\ndestinations / executors"]
    end

    subgraph State ["State Manager"]
        JSON[("migration_state.json\nportalocker")]
    end

    subgraph Portal ["Review Portal  :8000  (FastAPI)"]
        Dashboard["/teams/progress\nDashboard"]
        Review["/pipelines/id/review\nSide-by-side review"]
        Decision["Approve / Request Changes\nEscalate / Reject"]
    end

    Output[("output/team/pipeline/\npipeline.py\nmetadata.json")]
    LLM(["☁️ LLM\nAnthropic / Gateway / OpenAI"])

    User -->|"migrate ingest"| Ingest
    User -->|"migrate run"| Run
    User -->|"migrate serve"| Serve
    User -->|"migrate status"| Status
    User -->|"migrate export"| Export

    Ingest --> State
    Run --> Factory
    Factory --> Claude & Gateway & OpenAI
    Claude & Gateway & OpenAI --> LLM

    Run --> MigAgent
    MigAgent --> CtxBuilder
    CtxBuilder --> MigAgent
    MigAgent -->|"tool calls"| Classify & Lookup & Emit
    Lookup --> YAML
    MigAgent --> Output
    MigAgent --> State

    Serve --> Portal
    Dashboard & Review --> State
    Decision -->|"approve"| Output
    Decision -->|"request changes"| Run

    Export --> Output
```

---

### Agentic Loop (per pipeline)

```mermaid
sequenceDiagram
    actor User
    participant CLI as CLI (migrate run)
    participant Agent as MigrationAgent
    participant Ctx as context_builder
    participant LLM as ☁️ LLM (Anthropic/Gateway/OpenAI)
    participant Tools as Tool Dispatcher
    participant Catalog as StageCatalog (YAML)
    participant State as StateManager
    participant FS as output/ (filesystem)

    User->>CLI: migrate run --team alpha
    CLI->>State: get pending pipelines
    CLI->>Agent: migrate_pipeline(pipeline)

    Agent->>Ctx: build_migration_prompt(pipeline)
    Note over Ctx: full mode (~4000 tokens)<br/>or compact mode (~1500 tokens)
    Ctx-->>Agent: prompt

    Agent->>State: status = MIGRATING

    loop Up to 20 iterations
        Agent->>LLM: messages + system prompt + tool schemas
        Note over LLM: [LLM CALL] consumes tokens

        alt stop_reason = tool_use
            LLM-->>Agent: tool call: classify_pipeline(...)
            Agent->>Tools: dispatch classify_pipeline
            Note over Tools: [NO LLM] pure heuristic
            Tools-->>Agent: {format: "dlt", reasoning: "..."}
            Agent->>LLM: tool result

            LLM-->>Agent: tool call: lookup_stage_mapping(stage)
            Agent->>Tools: dispatch lookup_stage_mapping
            Tools->>Catalog: lookup(stage_name)
            Catalog-->>Tools: StageMapping (template, confidence)
            Tools-->>Agent: mapping result
            Agent->>LLM: tool result

            LLM-->>Agent: tool call: emit_migration_result(code, score, warnings)
            Agent->>Tools: dispatch emit_migration_result
            Note over Agent: capture final result
            Tools-->>Agent: {status: "received"}
            Agent->>LLM: tool result

        else stop_reason = end_turn
            LLM-->>Agent: done
        end
    end

    Agent->>FS: write pipeline.py + metadata.json
    Agent->>State: status = IN_REVIEW, store artifact
    Agent-->>CLI: GeneratedArtifact
    CLI-->>User: ✓ migrated (confidence: 0.85)

    User->>CLI: migrate serve
    Note over User: open http://localhost:8000
    User->>State: review pipeline (approve / request changes)

    alt approved
        State->>State: status = APPROVED
    else request changes
        State->>State: status = CHANGES_REQUESTED
        Note over User,Agent: re-migration triggered with feedback<br/>prior artifact injected into context
        User->>CLI: migrate run (re-migration)
    end
```

---

## Quickstart

### Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager — install with `pip install uv` or see [uv docs](https://docs.astral.sh/uv/getting-started/installation/)
- An API key for one of: Anthropic, OpenAI, or your enterprise AI gateway

### Setup

```bash
git clone git@github.com:heravelli/streamsets-databricks-migration-agent.git
cd streamsets-databricks-migration-agent

# Create and activate a virtual environment
uv venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# Install dependencies into the venv
uv sync

# Configure credentials
cp .env.example .env
# Edit .env — choose one of the three client options (anthropic / openai / gateway)
```

### Run

```bash
# 1. Place your StreamSets pipeline exports in data/pipelines/<team_name>/
mkdir -p data/pipelines/my_team
cp /path/to/exports/*.json data/pipelines/my_team/

# 2. Ingest pipelines (team name inferred from directory name)
uv run migrate ingest data/pipelines/my_team/

# 3. Run the migration agent
uv run migrate run --team my_team --concurrency 3

# 4. Start the review portal
uv run migrate serve

# 5. Open http://localhost:8000 in your browser to review and approve
```

---

## CLI Reference

| Command | Description |
|---|---|
| `migrate ingest <dir>` | Parse exports — team name inferred from directory name |
| `migrate ingest <dir> --team <name>` | Parse exports with explicit team name override |
| `migrate ingest <dir> --dry-run` | Parse only, don't write state |
| `migrate run --team <name>` | Run agent on all pending pipelines for a team |
| `migrate run --pipeline-id <id>` | Run agent on a single pipeline |
| `migrate run --all --concurrency 10` | Run all 2000 pipelines in parallel |
| `migrate serve [--port 8080] [--reload]` | Start the human review portal |
| `migrate status` | Progress table for all teams |
| `migrate status --team <name>` | Per-pipeline status for one team |
| `migrate export <output_dir>` | Bundle approved artifacts |
| `migrate export <output_dir> --team <name> --format zip` | Team-scoped zip bundle |

---

## Configuration

All settings are environment variables (`.env` file or container env):

```env
# ── Anthropic (direct) ─────────────────────────────────────────────────
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-6

# ── AI Gateway (alternative to direct Anthropic — see AI Gateway section)
AGENT_CLIENT_TYPE=gateway           # Set to "gateway" to use the gateway
AI_GATEWAY_URL=https://ai-gateway.internal/v1
AI_GATEWAY_TOKEN=your-token-here
AI_GATEWAY_MODEL=claude-sonnet      # Model name as exposed by the gateway

# ── Paths (override for Docker/AKS) ────────────────────────────────────
DATA_DIR=data/pipelines
OUTPUT_DIR=output
STATE_FILE=data/state/migration_state.json

# ── Agent tuning ────────────────────────────────────────────────────────
AGENT_MAX_TOKENS=8096
AGENT_CONCURRENCY=3                 # Parallel pipeline migrations
AGENT_COMPACT_CONTEXT=false         # Set true for token-limited gateway models
```

---

## AI Gateway

If your organisation routes AI traffic through a central gateway (LiteLLM, Azure AI Gateway, Apigee, etc.), use the gateway client instead of calling Anthropic directly.

Set these in `.env`:

```env
AGENT_CLIENT_TYPE=gateway
AI_GATEWAY_URL=https://ai-gateway.yourcompany.com/v1
AI_GATEWAY_TOKEN=<token-from-gateway-team>
AI_GATEWAY_MODEL=<model-name-as-exposed-by-gateway>
```

The gateway client speaks the **OpenAI-compatible** chat completions API (`POST /v1/chat/completions` with `tools`), which is the most common enterprise gateway standard. No code changes needed — the factory selects the right client based on `AGENT_CLIENT_TYPE`.

The direct Anthropic client (`AGENT_CLIENT_TYPE=anthropic`, the default) is unchanged.

---

## Stage Catalog

Stage mappings live in `catalog/stages/` as YAML files:

```
catalog/stages/
├── origins.yaml       # Sources (Kafka, JDBC, S3, etc.)
├── processors.yaml    # Transformations (expressions, type converters, etc.)
├── destinations.yaml  # Sinks (Delta, JDBC, Kafka, S3, etc.)
└── executors.yaml     # Executors (shell, JDBC query, email, etc.)
```

All **78 production stages** are fully cataloged (see `streamsets_stages.md` for the authoritative list). Each entry maps a StreamSets stage to its Databricks equivalent with a Jinja2 code template:

```yaml
- streamsets_stage: "com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource"
  streamsets_label: "Multi-Topic Kafka Consumer"
  databricks_equivalent: "dlt_streaming_table"
  confidence: "exact"             # exact | high | medium | low | unsupported
  code_template: |
    @dlt.table(name="{{ topic }}_raw")
    def {{ topic }}_raw():
        return spark.readStream.format("kafka")...
  config_mapping:
    conf.brokerURI: "bootstrap_servers"
    conf.topicList: "topic_list"
  requires_manual_review: false
```

To add a new stage discovered in production, add an entry to the appropriate YAML file and restart the agent. Stages with `confidence: "unsupported"` auto-route the pipeline to Notebook format.

---

## Human Review Portal

`uv run migrate serve` → http://localhost:8000

| Page | URL | Description |
|---|---|---|
| Dashboard | `/teams/progress` | All 20 teams — completion %, counts by status |
| Queue | `/pipelines/?team=X&status=in_review` | Reviewer's queue |
| Review | `/pipelines/{id}/review` | Side-by-side: original JSON + generated code |
| Diff | `/pipelines/{id}/diff` | What changed after a re-migration |

### Review decisions

- **Approve** → artifact written to `output/`, status = `approved`
- **Request Changes** → reviewer writes feedback → agent re-migrates in background → diff shown
- **Escalate** → routed to senior engineer queue
- **Reject** → marked rejected with reason

---

## Docker / AKS Deployment

### Local Docker

```bash
# Copy and fill in your credentials
cp .env.example .env

# Build and run
docker compose up --build

# Open http://localhost:8000
```

### Run CLI commands in Docker

```bash
# Ingest (one-off)
docker compose run --rm migration-agent uv run migrate ingest /data/pipelines/my_team/ --team my_team

# Run all migrations
docker compose run --rm migration-agent uv run migrate run --all --concurrency 5

# Check status
docker compose run --rm migration-agent uv run migrate status
```

### AKS (Kubernetes)

Mount a **PersistentVolumeClaim** at `/data` for state + output persistence.

```yaml
# Example PVC mount in your deployment
volumes:
  - name: migration-data
    persistentVolumeClaim:
      claimName: migration-pvc

volumeMounts:
  - name: migration-data
    mountPath: /data
```

Environment variables from a Kubernetes Secret:
```bash
kubectl create secret generic migration-secrets \
  --from-literal=ANTHROPIC_API_KEY=sk-ant-... \
  --from-literal=AI_GATEWAY_TOKEN=...
```

The review portal pod runs the FastAPI server. CLI migration commands (`migrate run`) run as **Kubernetes Jobs**.

---

## Project Structure

```
├── agent/
│   ├── claude_client.py        # Direct Anthropic SDK client
│   ├── gateway_client.py       # AI gateway client (OpenAI-compatible)
│   ├── client_factory.py       # Selects client based on AGENT_CLIENT_TYPE
│   ├── migration_agent.py      # Agentic loop (tool-use orchestrator)
│   ├── context_builder.py      # Builds per-pipeline Claude context
│   ├── tools/                  # classify_pipeline, lookup_stage_mapping, emit_result
│   └── prompts/system_prompt.md
├── catalog/stages/             # YAML stage mappings (origins/processors/destinations/executors)
├── cli/                        # Typer CLI commands
├── config/settings.py          # All config from env vars
├── models/                     # Pydantic models (streamsets, migration, stage_mapping)
├── parser/                     # Pipeline JSON/YAML parser + classifier
├── review/                     # FastAPI review portal + HTML templates
├── state/                      # File-based state manager (portalocker)
├── data/pipelines/             # Input: StreamSets exports (gitignored)
├── output/                     # Generated Databricks code (gitignored)
├── Dockerfile
└── docker-compose.yml
```

---

## Pipeline Status Lifecycle

```
pending → migrating → in_review → approved ✓
                              ↓
                     changes_requested → re_migrating → in_review
                              ↓
                          escalated
                              ↓
                           rejected
```

---

## License

Internal use.
