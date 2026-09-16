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
