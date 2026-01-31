# WARNING: this should be built from the parent directory or using the docker-compose file.

FROM astral/uv:0.9.25-python3.11-alpine

# Install build dependencies for Cassandra driver C extensions
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    libev-dev \
    libffi-dev

WORKDIR /app

COPY module-01-pipeline/pipeline.py .
COPY .python-version pyproject.toml .

ENV UV_NO_DEV "1"
ENV UV_NO_SYNC "1"

RUN uv sync

ENTRYPOINT ["uv", "run", "pipeline.py"]

CMD ["--help"]