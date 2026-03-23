FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Use system Python (python:3.12-slim) — avoids uv downloading its own Python at runtime
ENV UV_SYSTEM_PYTHON=1

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock* ./

# Install dependencies (no dev deps in production image)
RUN uv sync --no-dev

# Copy application code
COPY . .

# Configure default paths — override via env vars or volume mounts in AKS
ENV DATA_DIR=/data/pipelines \
    OUTPUT_DIR=/data/output \
    STATE_FILE=/data/state/migration_state.json \
    CATALOG_DIR=/app/catalog/stages \
    PROMPTS_DIR=/app/agent/prompts \
    REVIEW_HOST=0.0.0.0 \
    REVIEW_PORT=8000

# Ensure data directories exist (actual data comes from mounted volumes in AKS)
RUN mkdir -p /data/pipelines /data/state /data/output

EXPOSE 8000

# Health check for Kubernetes liveness/readiness probes
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uv", "run", "migrate", "serve", "--host", "0.0.0.0", "--port", "8000"]
