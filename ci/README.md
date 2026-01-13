# Integration tests (CI)

This folder contains integration tests for xoverrr using real databases
started via Docker.

## Prerequisites
- Linux/Mac environment
- Brew
- Docker
- Docker Compose
- Colima
- Python 3.9+
- Installed project dependencies

```bash
brew install colima docker docker-compose lima-additional-guestagents   
```
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