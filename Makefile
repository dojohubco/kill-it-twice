.PHONY: verify-m1 verify

verify-m1:
	@echo 'M1 ONLY: source contract checks; not G1 or G2 acceptance'
	npm run typecheck
	npm run lint
	npm run test:unit
	npm run test:integration:m1

verify:
	@echo 'G1 NOT IMPLEMENTED'
	@echo 'G2 NOT IMPLEMENTED'
	@echo 'G3 NOT IMPLEMENTED'
	@echo 'G4 NOT IMPLEMENTED'
	@echo 'G5 NOT IMPLEMENTED'
	@exit 1
