FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci --no-audit --no-fund --ignore-scripts
COPY src ./src
COPY apps/control-api ./apps/control-api
RUN ./node_modules/.bin/tsc -p apps/control-api/tsconfig.json
USER node
CMD ["node", "artifacts/control-api-build/apps/control-api/main.js"]
