PYTHON ?= python
COMPOSE_LOCAL = docker compose -f tests/integration/docker/docker-compose.yml
COMPOSE_CI = docker compose -f tests/integration/docker/docker-compose.ci.yml

.PHONY: help lint test-unit test-integration up-dbs down-dbs up-dbs-ci down-dbs-ci build

help:
	@echo "lint               ruff check and format check"
	@echo "test-unit          pytest tests/unit"
	@echo "test-integration   pytest tests/integration (databases must be up)"
	@echo "up-dbs             start local docker databases"
	@echo "down-dbs           stop local docker databases"
	@echo "up-dbs-ci          start CI docker databases"
	@echo "down-dbs-ci        stop CI docker databases"
	@echo "build              build sdist and wheel"

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m ruff format --check src tests

test-unit:
	$(PYTHON) -m pytest tests/unit --tb=short

test-integration:
	$(PYTHON) -m pytest tests/integration --tb=short

up-dbs:
	$(COMPOSE_LOCAL) up -d --wait

down-dbs:
	$(COMPOSE_LOCAL) down -v

up-dbs-ci:
	$(COMPOSE_CI) up -d --wait --wait-timeout 600

down-dbs-ci:
	$(COMPOSE_CI) down -v

build:
	$(PYTHON) -m build
