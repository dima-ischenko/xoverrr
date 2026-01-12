# Integration tests (CI)

This folder contains integration tests for xoverrr using real databases
started via Docker.

## Prerequisites
- Docker
- Docker Compose
- Python 3.9+
- Installed project dependencies

---

## Start databases

```bash
docker-compose -f docker/docker-compose.yml up -d
```

## Restart databases (clean state)

```bash
docker-compose -f docker/docker-compose.yml down -v
docker-compose -f docker/docker-compose.yml up -d
```

## Run all/pair/single integration tests
```bash
pytest tests
pytest tests/test_oracle_postgres_compare.py
pytest tests/test_oracle_postgres_compare.py::TestOraclePostgresComparison::test_compare_counts_success
```