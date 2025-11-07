FROM oven/bun:1.2.5-alpine AS base

WORKDIR /app

FROM base AS deps
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM base AS build
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM base AS runtime
RUN addgroup --system --gid 1001 ca_embed
RUN adduser --system --uid 1001 ca_embed

COPY --from=deps --chown=ca_embed:ca_embed /app/node_modules ./node_modules
COPY --from=build --chown=ca_embed:ca_embed /app/dist ./dist
COPY --from=build --chown=ca_embed:ca_embed /app/src ./src
COPY --from=build --chown=ca_embed:ca_embed /app/package.json ./

RUN mkdir -p data && chown ca_embed:ca_embed data

USER ca_embed

EXPOSE 3000 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:3000/health || exit 1

CMD ["bun", "run", "src/index.ts"]