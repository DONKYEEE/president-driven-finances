FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra ingestion --no-install-project

COPY README.md ./
COPY ingestion ./ingestion
COPY etl ./etl
COPY ai ./ai
RUN uv sync --frozen --no-dev --extra ingestion

CMD ["uv", "run", "--no-sync", "ingest", "loop"]
