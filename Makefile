.PHONY: quality verify-m1 verify

quality:
	npm run format:check
	npm run lint
	npm run typecheck
	npm run knip
	npm run validate:compose
	npm run validate:workflow
	npm run test:unit

verify-m1: quality
	@echo 'M1/M1.1 ONLY: fresh source acceptance; G1-G5 remain NOT IMPLEMENTED'
	npm run test:integration:m1

verify:
	@echo 'G1 NOT IMPLEMENTED'
	@echo 'G2 NOT IMPLEMENTED'
	@echo 'G3 NOT IMPLEMENTED'
	@echo 'G4 NOT IMPLEMENTED'
	@echo 'G5 NOT IMPLEMENTED'
	@exit 1

.PHONY: verify-m2a
verify-m2a: quality
	npm run test:integration:m2a

.PHONY: verify-m2b
verify-m2b: quality
	npm run test:integration:m1
	npm run test:integration:m2a
	npm run test:integration:m2b
	npm run test:integration:m2b -- --upgrade

.PHONY: verify-m2c
verify-m2c: verify-m2b
	npm run test:integration:m2c
	npm run test:integration:m2c -- --upgrade

.PHONY: verify-m2c1
verify-m2c1: verify-m2c
	npm run test:integration:m2c1
	npm run test:integration:m2c1 -- --upgrade

.PHONY: verify-m3
verify-m3:
	npm run verify:m3

.PHONY: verify-m4
verify-m4: verify-m3
	npm run test:integration:m4
	npm run test:integration:m4 -- --upgrade

.PHONY: verify-m4-1
verify-m4-1: verify-m4
	npm run test:reproduction:m41
	npm run test:integration:m41
	npm run test:integration:m41 -- --upgrade

.PHONY: verify-m5a
verify-m5a: verify-m4-1
	npm run test:integration:m5a
	npm run test:integration:m5a -- --upgrade

.PHONY: verify-m5b
verify-m5b: verify-m5a
	npm run test:integration:m5b
	npm run test:integration:m5b -- --upgrade
	npm run test:integration:m5b-empty

.PHONY: verify-m6
verify-m6: verify-m5b
	npm run test:integration:m6
	npm run test:integration:m6 -- --upgrade

.PHONY: verify-ui
verify-ui: quality
	npm run build:ui
	npm run test:ui

.PHONY: up down seed runtime-status operator-token
SEED_COUNT ?= 1024
up:
	npm run runtime:preflight
	docker compose up -d --build
down:
	docker compose down
seed:
	docker compose run --rm --no-deps seed node scripts/runtime/seed.ts $(SEED_COUNT)
runtime-status:
	docker compose run --rm --no-deps inspect
operator-token:
	@docker compose run --rm --no-deps inspect node scripts/runtime/inspect.ts token

.PHONY: verify-runtime
verify-runtime: quality
	npm run verify:runtime

.PHONY: verify-functional
verify-functional: quality
	npm run verify:functional
