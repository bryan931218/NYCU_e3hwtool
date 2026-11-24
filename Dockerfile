FROM node:22-bookworm-slim AS node_runtime
FROM python:3.12-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt requirements-build.txt requirements-security-constraints.txt ./
RUN python -m pip install -r requirements-build.txt \
    && python -m pip install -r requirements.txt \
    && python -m playwright install --with-deps --only-shell chromium
COPY . .
CMD ["sh", "-c", "exec gunicorn --chdir backend --bind 0.0.0.0:${PORT:-8000} --workers 1 --threads 8 --timeout 240 wsgi:app"]
