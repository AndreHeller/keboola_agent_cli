.DEFAULT_GOAL := help

.PHONY: help install sync test test-unit test-integration test-file lint lint-fix format format-check check clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install in development mode (editable)
	uv pip install -e ".[dev]"

sync: ## Sync dependencies from lockfile
	uv sync

test: ## Run all tests
	uv run pytest tests/ -v

test-unit: ## Run unit tests only (exclude integration)
	uv run pytest tests/ -v -m "not integration"

test-integration: ## Run integration tests only
	uv run pytest tests/ -v -m integration

test-file: ## Run a specific test file (FILE=tests/test_cli.py)
	uv run pytest $(FILE) -v

lint: ## Run ruff linter
	uv run ruff check .

lint-fix: ## Run ruff linter with auto-fix
	uv run ruff check . --fix

format: ## Format code with ruff
	uv run ruff format .

format-check: ## Check code formatting (no changes)
	uv run ruff format . --check

check: lint format-check test ## Run all checks (lint + format + test)

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
