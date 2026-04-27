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

COPY --from=deps /app/node_modules ./node_modules
COPY package.json bun.lock tsconfig.json ./
COPY index.ts ./
COPY src ./src

RUN mkdir -p /app/output
VOLUME ["/app/output"]

ENTRYPOINT ["bun", "run", "index.ts"]
CMD ["--help"]
