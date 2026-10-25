# syntax=docker/dockerfile:1.7
# One image, many roles (processor, producer, transform, api, bootstrap, verify, maintain).

ARG PYTHON_IMAGE=python:3.12-slim-trixie

FROM ${PYTHON_IMAGE} AS build
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /src
COPY requirements.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /opt/venv && uv pip install --python /opt/venv/bin/python -r requirements.lock
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python /opt/venv/bin/python --no-deps . \
 && /opt/venv/bin/floe --version

FROM ${PYTHON_IMAGE} AS runtime
LABEL org.opencontainers.image.source="https://github.com/sushmit-gujar/floe" \
      org.opencontainers.image.description="floe: streaming Iceberg lakehouse on Kubernetes" \
      org.opencontainers.image.licenses="MIT"
# Drop pip/setuptools from the runtime (not needed, and a common source of scanner findings).
RUN python -m pip uninstall -y pip setuptools wheel >/dev/null 2>&1 || true \
 && groupadd --gid 10001 floe && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin floe
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
USER 10001:10001
WORKDIR /tmp
ENTRYPOINT ["floe"]
CMD ["--help"]
