# Variables
PACKAGE_NAME := bptree-lean
BINARY_NAME := bptree-lean
export PATH := $(HOME)/.elan/bin:$(PATH):/nix/store/94hz3k8jfjmh8p9qzn1in7svkhl4f8sw-elan-4.2.4/bin
LAKE := $(shell command -v lake 2>/dev/null || (command -v elan >/dev/null 2>&1 && elan which lake 2>/dev/null) || echo "lake")
ELAN := $(shell command -v elan 2>/dev/null || echo "elan")
SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show help messages for all available targets
	@grep -E '^[a-zA-Z_-]+:.*## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the Lean project and default executable
	@echo "Building $(PACKAGE_NAME)..."
	@$(LAKE) build

.PHONY: test
test: ## Run the Lean test suite
	@echo "Running Lean tests..."
	@$(LAKE) test

.PHONY: run
run: ## Run the main entry point (Main.lean) in the Lake environment
	@echo "Running Main.lean..." >&2
	@$(LAKE) env lean --run Main.lean

.PHONY: cache-get
cache-get: ## Download precompiled Mathlib build cache
	@echo "Fetching Mathlib build cache..."
	@$(LAKE) exe cache get

.PHONY: check
check: ## Verify build targets and test driver configuration
	@echo "Checking Lake build and test configuration..."
	@$(LAKE) check-build
	@$(LAKE) check-test

.PHONY: lint
lint: ## Build with warnings treated as errors
	@echo "Linting Lean sources..."
	@$(LAKE) --wfail build

.PHONY: clean
clean: ## Remove package build artifacts
	@echo "Cleaning build artifacts..."
	@$(LAKE) clean «bptree-lean»
	@rm -rf .lake/build/lib/Bptree* .lake/build/ir/Bptree* .lake/build/lib/Tests* .lake/build/ir/Tests*
