# syntax=docker/dockerfile:1.7

# ── deps stage: install only production dependencies ────────────────────────
FROM oven/bun:1 AS deps
WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

# ── runtime stage ───────────────────────────────────────────────────────────
FROM oven/bun:1 AS runtime
WORKDIR /app

ENV NODE_ENV=production
ENV BROWSER_USE_PYTHON=/opt/venv/bin/python
ENV BROWSER_USE_HEADLESS=true
ENV BROWSER_USE_USER_DATA_DIR=/tmp/browser-use-profile
ENV LOG_FILE=/app/logs/schoolyank.log
ENV STATUS_CSV_PATH=/app/output/status.csv
ENV SCHOOLYANK_INPUT_CSV=/app/input/schools_with_staff_urls.csv
ENV PATH=/opt/venv/bin:$PATH

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
  && rm -rf /var/lib/apt/lists/*

COPY --from=deps /app/node_modules ./node_modules
COPY package.json bun.lock tsconfig.json requirements.txt ./

RUN python3 -m venv /opt/venv \
  && python -m pip install --no-cache-dir -r requirements.txt \
  && browser-use install

COPY index.ts ./
COPY src ./src
COPY scripts ./scripts

RUN chmod +x /app/scripts/docker-entrypoint.sh \
  && mkdir -p /app/input /app/output /app/schools /app/logs /tmp/browser-use-profile \
  && chmod -R 777 /app/input /app/output /app/schools /app/logs /tmp/browser-use-profile
VOLUME ["/app/input", "/app/output", "/app/schools", "/app/logs"]

ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]
CMD []
