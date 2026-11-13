MY_ISV_SUITES := bare_metal control-plane iam image-registry network observability security storage vm
DEMO_TARGETS := $(addprefix demo-,$(MY_ISV_SUITES))

.PHONY: help pre-commit build test coverage clean lint format install bump-patch bump-fix bump-minor bump-feat bump-major bump bump-check \
	security-trivy security-trivy-detail security-trufflehog ci-security demo-test demo-all $(DEMO_TARGETS) plan plan-coverage validate-suites \
	reqcheck vendor vendor-pjdfstest vendor-storage-scale-test clean-vendor

PACKAGES := isvctl isvreporter isvtest
BUMP_SCRIPT := scripts/bump-version.py
RUFF_VERSION := 0.15.12
RUFF := uvx --from ruff==$(RUFF_VERSION) ruff

# Vendored pjdfstest
PJDFSTEST_REPO := https://github.com/pjd/pjdfstest.git
PJDFSTEST_REV := ededbeb2b44929972898afb87474b0937f78a877
PJDFSTEST_DIR := isvtest/vendor/pjdfstest

# Vendored storage-scale-test (source release; the Kubernetes sweep runs elbencho from the image the release pins)
STORAGE_SCALE_TEST_VERSION := v2.1.0
STORAGE_SCALE_TEST_SHA256 := 35d8c5085da51f4cc1822e02fa89c3228fb9e1b83e480506bea80f4ab201472f
STORAGE_SCALE_TEST_TARBALL := storage-scale-test-$(STORAGE_SCALE_TEST_VERSION)-source.tar.gz
STORAGE_SCALE_TEST_URL := https://github.com/NVIDIA/storage-scale-test/releases/download/$(STORAGE_SCALE_TEST_VERSION)/$(STORAGE_SCALE_TEST_TARBALL)
STORAGE_SCALE_TEST_DIR := isvtest/vendor/storage-scale-test

# DISABLED (2026-03-24): Trivy supply chain compromise - see GHSA-69fq-xp46-6x23.
# Do NOT pull aquasec/trivy images from Docker Hub until Aqua Security regains control.
# When re-enabling, pin by digest and verify against a trusted source (GitHub release, not Docker Hub).
# TRIVY_IMAGE ?= aquasec/trivy@sha256:bcc376de8d77cfe086a917230e818dc9f8528e3c852f7b1aff648949b6258d1c  # 0.69.3 (last known-good release)
TRUFFLEHOG_IMAGE ?= trufflesecurity/trufflehog:latest
TRUFFLEHOG_EXCLUDE ?= .trufflehog-exclude
# Lob's key pattern is `test_` + 35 chars, so its verifier reports every 40-character
# pytest function name in the repo as a verified secret. Nothing here uses Lob (a postal
# mail API), so the detector can only produce false positives.
TRUFFLEHOG_EXCLUDE_DETECTORS ?= lob
SECURITY_SKIP_DIRS := .git,dist,htmlcov,.pytest_cache,.ruff_cache,.venv,node_modules,vendor,.terraform
# TRIVY_SARIF ?= vulnerability-scan-results.sarif

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

pre-commit: ## Run pre-commit on all packages
	@echo "Running pre-commit on all packages..."
	@for pkg in $(PACKAGES); do \
		echo ""; \
		echo "=========================================="; \
		echo "Running pre-commit in $$pkg"; \
		echo "=========================================="; \
		if [ -n "$$GITLAB_CI" ]; then \
			(cd $$pkg && uvx pre-commit run -a --show-diff-on-failure) || exit 1; \
		else \
			(cd $$pkg && uvx pre-commit run -a) || exit 1; \
		fi; \
	done
	@echo ""
	@echo "✅ All pre-commit checks passed!"

lint: ## Run ruff linting on all packages
	@for pkg in $(PACKAGES); do \
		echo "Linting $$pkg..."; \
		(cd $$pkg && $(RUFF) check src/) || exit 1; \
	done

format: ## Format code with ruff on all packages
	@for pkg in $(PACKAGES); do \
		echo "Formatting $$pkg..."; \
		(cd $$pkg && $(RUFF) format src/) || exit 1; \
	done

# DISABLED (2026-03-24): Trivy supply chain compromise - see GHSA-69fq-xp46-6x23.
# security-trivy: ## Trivy fs scan (HIGH/CRITICAL; Docker). Writes $(TRIVY_SARIF), prints per-finding summary; fails on un-ignored findings
# security-trivy-detail: ## List each finding from $(TRIVY_SARIF) (jq)
security-trivy security-trivy-detail:
	@echo "ERROR: Trivy targets are disabled due to active supply chain compromise (GHSA-69fq-xp46-6x23)." >&2
	@echo "See: https://github.com/advisories/GHSA-69fq-xp46-6x23" >&2
	@exit 1

security-trufflehog: ## Run TruffleHog secret scan (Docker; verified/unknown, --only-verified). Exits non-zero if verified secrets are found
	@command -v docker >/dev/null 2>&1 || { echo "docker not found; install Docker to run local security scans"; exit 1; }
	docker run --rm -v "$(CURDIR):/work" -w /work $(TRUFFLEHOG_IMAGE) filesystem /work \
		--exclude-paths /work/$(TRUFFLEHOG_EXCLUDE) \
		--exclude-detectors $(TRUFFLEHOG_EXCLUDE_DETECTORS) \
		--results=verified,unknown --only-verified --fail

ci-security: security-trufflehog ## Run local equivalents of CI security scans (Trivy disabled - GHSA-69fq-xp46-6x23)

test: ## Run tests for all packages
	@for pkg in $(PACKAGES); do \
		echo "Testing $$pkg..."; \
		if [ "$$pkg" = "isvtest" ]; then \
			(cd $$pkg && uv run pytest -m unit) || exit 1; \
		else \
			(cd $$pkg && uv run pytest) || exit 1; \
		fi; \
	done
	@echo "Testing scripts..."
	@uv run pytest scripts/tests || exit 1
	@echo ""
	@echo "✅ All tests passed!"

# run_demo,<suite>[,<capability>] - a plain suite with no capability runs only
# its core checks, so suites holding gated checks name one to exercise those
# stubs. Unlisted suites are core-only or platform suites.
DEMO_CAP_network       := vm
DEMO_CAP_observability := vm
DEMO_CAP_security      := vm
DEMO_CAP_storage       := vm

define run_demo
	@echo ""
	@echo "=========================================="
	@echo "Demo test: $(1)$(if $(2), --capability $(2),)"
	@echo "=========================================="
	@echo "Running cmd: ISVCTL_DEMO_MODE=1 uv run isvctl test run -f isvctl/configs/providers/my-isv/config/$(1).yaml$(if $(2), --capability $(2),)"
	@ISVCTL_DEMO_MODE=1 uv run isvctl test run \
		-f isvctl/configs/providers/my-isv/config/$(1).yaml \
		$(if $(2),--capability $(2),)
endef

demo-test: demo-all ## Alias for demo-all (backward compat)

demo-all: ## Run all my-isv living examples (or demo-<suite> for one, e.g. demo-security)
	@echo "Running my-isv living examples in demo mode..."
	@for suite in $(MY_ISV_SUITES); do \
		$(MAKE) --no-print-directory demo-$$suite || exit 1; \
	done
	@echo ""
	@echo "✅ All my-isv living examples passed in demo mode!"
	@echo "Suites: $(MY_ISV_SUITES)"

# image-registry and storage are excluded here because they have their own rules
# below; leaving them in would make every make invocation warn about an overridden recipe.
$(filter-out demo-image-registry demo-storage,$(DEMO_TARGETS)): demo-%:
	$(call run_demo,$*,$(DEMO_CAP_$*))

# image-registry splits its gated checks between vm and bare_metal, so one run
# cannot reach both.
demo-image-registry:
	$(call run_demo,image-registry,vm)
	$(call run_demo,image-registry,bare_metal)

# storage also runs its opt-in performance overlay, which replaces the step list, so it
# is a second run. --label performance keeps the suite's live Kubernetes probes out, and
# KUBECONFIG=/dev/null keeps the demo off whatever cluster the shell points at.
demo-storage:
	$(call run_demo,storage,$(DEMO_CAP_storage))
	@echo ""
	@echo "=========================================="
	@echo "Demo test: storage + overlays/storage-perf.yaml --capability kubernetes --label performance"
	@echo "=========================================="
	@echo "Running cmd: ISVCTL_DEMO_MODE=1 KUBECONFIG=/dev/null uv run isvctl test run -f isvctl/configs/providers/my-isv/config/storage.yaml -f isvctl/configs/providers/my-isv/config/overlays/storage-perf.yaml --capability kubernetes --label performance"
	@ISVCTL_DEMO_MODE=1 KUBECONFIG=/dev/null uv run isvctl test run \
		-f isvctl/configs/providers/my-isv/config/storage.yaml \
		-f isvctl/configs/providers/my-isv/config/overlays/storage-perf.yaml \
		--capability kubernetes --label performance

coverage: ## Run tests with coverage and generate combined report
	@echo "Running tests with coverage..."
	@for pkg in $(PACKAGES); do \
		echo "Testing $$pkg with coverage..."; \
		if [ "$$pkg" = "isvtest" ]; then \
			(cd $$pkg && uv run pytest -m unit --cov=src --cov-report=) || exit 1; \
		else \
			(cd $$pkg && uv run pytest --cov=src --cov-report=) || exit 1; \
		fi; \
	done
	@echo ""
	@echo "Combining coverage reports..."
	@uv run coverage combine $(foreach pkg,$(PACKAGES),$(pkg)/.coverage)
	@uv run coverage xml -o coverage-combined.xml
	@uv run coverage report
	@echo ""
	@echo "✅ Coverage report generated: coverage-combined.xml"

build: ## Build all packages (wheels output to dist/)
	@echo "Building wheel packages..."
	@mkdir -p dist
	@for pkg in $(PACKAGES); do \
		echo "Building $$pkg..."; \
		uv build $$pkg/ --out-dir dist/ --no-build-logs || exit 1; \
	done
	@echo ""
	@echo "✅ All packages built successfully!"
	@echo "Wheels available in dist/"

install: ## Install all packages in development mode
	uv sync
	@echo ""
	@echo "✅ Installation complete!"

bump-patch: ## Bump patch version (e.g. 0.4.2 -> 0.4.3)
	uv run python $(BUMP_SCRIPT) patch

bump-fix: bump-patch ## Alias for bump-patch

bump-minor: ## Bump minor version (e.g. 0.4.2 -> 0.5.0)
	uv run python $(BUMP_SCRIPT) minor

bump-feat: bump-minor ## Alias for bump-minor

bump-major: ## Bump major version (e.g. 0.4.2 -> 1.0.0)
	uv run python $(BUMP_SCRIPT) major

bump: ## Bump to explicit version (e.g. make bump VERSION=1.2.3)
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump VERSION=x.y.z"; exit 1; fi
	uv run python $(BUMP_SCRIPT) $(VERSION)

bump-check: ## Verify versions match (CI; e.g. make bump-check VERSION=1.2.3)
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump-check VERSION=x.y.z"; exit 1; fi
	uv run python $(BUMP_SCRIPT) --check $(VERSION)

clean: ## Clean build artifacts and test outputs
	@echo "Cleaning build artifacts and test outputs..."
	@rm -rf dist/
	@rm -f coverage-combined.xml .coverage
	@for pkg in $(PACKAGES); do \
		echo "Cleaning $$pkg..."; \
		rm -rf $$pkg/dist/ $$pkg/.pytest_cache/ $$pkg/__pycache__/; \
		rm -f $$pkg/junit.xml $$pkg/coverage.xml $$pkg/.coverage; \
		find $$pkg -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true; \
		find $$pkg -type f -name "*.pyc" -delete 2>/dev/null || true; \
	done
	@echo "Cleaning scripts..."
	@rm -f scripts/.coverage
	@find scripts -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find scripts -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find scripts -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Clean complete!"

update-spdx-headers: ## Update SPDX headers in all packages
	@echo "Updating SPDX headers in all packages..."
	@uv run python scripts/add_spdx_headers.py
	@echo "✅ SPDX headers updated!"

plan: ## Render docs/test-plan.yaml + requirements mapping to AsciiDoc
	@echo "Rendering test plan..."
	@uv run python scripts/test_plan_yaml_to_adoc.py
	@echo "Rendering requirements traceability mapping..."
	@uv run python scripts/requirements_matrix_to_adoc.py
	@echo "Rendering source requirement listings (yaml -> md)..."
	@uv run python scripts/requirements_source_to_md.py
	@echo "✅ Test plan + requirements mapping + source listings rendered!"

reqcheck: ## Validate requirements <-> test mapping consistency
	@uv run python scripts/reqtrace.py validate

plan-coverage: ## Report test-plan coverage + gaps via wired test_ids (CHECK=1 for pre-commit guardrail)
	@uv run python scripts/test_plan_coverage.py $(if $(CHECK),--check,)

validate-suites: ## Require test_id and labels on every suite check (CHECK=1 for CI/pre-commit)
	@uv run python scripts/validate_suite_wiring.py $(if $(CHECK),--check,)

vendor: vendor-pjdfstest vendor-storage-scale-test ## Download every pinned third-party test tool into isvtest/vendor/

clean-vendor: ## Remove the vendored test tools (`make vendor` downloads them again)
	@rm -rf $(PJDFSTEST_DIR) $(STORAGE_SCALE_TEST_DIR) $(STORAGE_SCALE_TEST_DIR).tmp
	@echo "✅ Removed $(PJDFSTEST_DIR) and $(STORAGE_SCALE_TEST_DIR)"

vendor-pjdfstest: ## Download pinned pjdfstest source into isvtest/vendor/ (gitignored; required by K8sPosixComplianceCheck)
	@rm -rf $(PJDFSTEST_DIR)
	@git clone --quiet $(PJDFSTEST_REPO) $(PJDFSTEST_DIR)
	@git -C $(PJDFSTEST_DIR) checkout --quiet $(PJDFSTEST_REV)
	@echo "✅ Vendored pjdfstest @ $(PJDFSTEST_REV) -> $(PJDFSTEST_DIR)"

vendor-storage-scale-test: ## Download pinned storage-scale-test release into isvtest/vendor/ (gitignored; required by the storage performance checks)
	@command -v sha256sum >/dev/null || command -v shasum >/dev/null \
		|| { echo "❌ need sha256sum or shasum to verify $(STORAGE_SCALE_TEST_TARBALL)"; exit 1; }
	@rm -rf $(STORAGE_SCALE_TEST_DIR).tmp && mkdir -p $(STORAGE_SCALE_TEST_DIR).tmp
	@curl -fsSL --retry 3 -o $(STORAGE_SCALE_TEST_DIR).tmp/$(STORAGE_SCALE_TEST_TARBALL) $(STORAGE_SCALE_TEST_URL) \
		|| { rm -rf $(STORAGE_SCALE_TEST_DIR).tmp; echo "❌ could not download $(STORAGE_SCALE_TEST_URL)"; exit 1; }
	@sha=$$(command -v sha256sum >/dev/null && echo sha256sum || echo "shasum -a 256"); \
		echo "$(STORAGE_SCALE_TEST_SHA256)  $(STORAGE_SCALE_TEST_DIR).tmp/$(STORAGE_SCALE_TEST_TARBALL)" | $$sha -c --quiet - \
		|| { rm -rf $(STORAGE_SCALE_TEST_DIR).tmp; echo "❌ $(STORAGE_SCALE_TEST_TARBALL) checksum mismatch"; exit 1; }
	@tar -xzf $(STORAGE_SCALE_TEST_DIR).tmp/$(STORAGE_SCALE_TEST_TARBALL) -C $(STORAGE_SCALE_TEST_DIR).tmp --strip-components=1 --no-same-owner \
		|| { rm -rf $(STORAGE_SCALE_TEST_DIR).tmp; echo "❌ could not extract $(STORAGE_SCALE_TEST_TARBALL)"; exit 1; }
	@rm -f $(STORAGE_SCALE_TEST_DIR).tmp/$(STORAGE_SCALE_TEST_TARBALL)
	@version=$$(bash $(STORAGE_SCALE_TEST_DIR).tmp/lib/project_version.sh resolve $(STORAGE_SCALE_TEST_DIR).tmp); \
		[ "$$version" = "$(STORAGE_SCALE_TEST_VERSION)" ] \
		|| { rm -rf $(STORAGE_SCALE_TEST_DIR).tmp; echo "❌ extracted storage-scale-test reports version $$version, expected $(STORAGE_SCALE_TEST_VERSION)"; exit 1; }
	@rm -rf $(STORAGE_SCALE_TEST_DIR) && mv $(STORAGE_SCALE_TEST_DIR).tmp $(STORAGE_SCALE_TEST_DIR)
	@out=$$(uv run python $(STORAGE_SCALE_TEST_DIR)/lib/python_bootstrap.py $(STORAGE_SCALE_TEST_DIR) 2>&1) \
		|| { echo "$$out"; echo "❌ could not prepare storage-scale-test's Python environment"; exit 1; }
	@echo "✅ Vendored storage-scale-test @ $(STORAGE_SCALE_TEST_VERSION) -> $(STORAGE_SCALE_TEST_DIR)"

.PHONY: changelog-fill
changelog-fill: ## Fill CHANGELOG.md gaps via an LLM CLI (CLI=auto|cursor|codex|claude)
	@scripts/changelog-fill.sh $(CLI)
