# Unit Tests

## Installation & Setup

### Create and Activate Virtual Environment

```bash
# Create virtual environment in project root
python3 -m venv venv

# Activate it (Linux/macOS)
source venv/bin/activate

# Check Python version (should be 3.9+)
python --version
```

### Install Dependencies

```bash
# Navigate to the root directory
# Install package in development mode with test dependencies
pip install -e ".[dev,test]"

# If pip command is not found, use pip3
# pip3 install -e ".[dev,test]"

# Or install all dependencies separately
pip install -e .
pip install pytest pytest-cov
```

### Virtual Environment Issues
If package imports fail:
```bash
# Ensure you're in the project root
cd /path/to/xoverrr

# Reactivate virtual environment
deactivate
source venv/bin/activate

# Reinstall in development mode
pip install -e ".[dev,test]"
```

### Python Command Issues
If `python` command is not found:
```bash
# Use python3 explicitly
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -e ".[dev,test]"
python3 -m pytest tests -v
```

```bash
# Remove virtual environment (when done)
deactivate
rm -rf venv/
```

## Running unit Tests

From the project root directory (with virtual environment activated):

```bash
# Run all integration tests
python -m pytest tests/unit -v

# Run specific test file
python -m pytest tests/unit/test_utils.py -v

# Run specific test method
pytest tests/unit/test_utils.py::TestUtils::test_duplicate_primary_keys_in_target -v 

# Run with coverage report
python -m pytest tests/unit --cov=src --cov-report=html -v
```


# Integration Tests (CI)

This sections contains integration tests for xoverrr using real databases started via Docker.

## Prerequisites

- **Linux/macOS environment**
- **Python 3.9+** with `venv` module available
- **Docker**
- **Docker Compose**

### macOS-specific Requirements (Apple Silicon M1/M2/M3 processors)
For macOS with Apple Silicon (M1/M2/M3) processors, Oracle database requires x86_64 architecture. Use Colima to run x86_64 containers:

```bash
# Install required tools via Homebrew
brew install colima docker docker-compose

# Start Colima with x86_64 architecture
colima start --arch x86_64 --memory 8 --cpu 4 --disk 40 --vm-type=vz --mount-type=sshfs


# Set Docker context to Colima
docker context use colima
```

**Note for Linux/Intel Mac users:** Colima is not required. You can use Docker Desktop or native Docker directly.


### 3. Start Test Databases

```bash
# Navigate to the integration directory
cd tests/integration

# Start all databases in detached mode
docker-compose -f docker/docker-compose.yml up -d

# Wait for databases to be ready (healthchecks will complete)
# Check status
docker-compose -f docker/docker-compose.yml ps
```

### 4. Reset Databases (Clean State)

```bash
# Stop and remove containers with volumes
docker-compose -f docker/docker-compose.yml down -v

# Restart fresh
docker-compose -f docker/docker-compose.yml up -d
```

## Running integration Tests

From the project root directory (with virtual environment activated):

```bash
# Run all integration tests
python -m pytest tests/integration -v

# Run specific test file
python -m pytest tests/integration/data_types/test_data_types.py -v

# Run specific test method
python -m pytest tests/integration/test_edge_cases.py::TestCustomQueryComparison::test_custom_query_comparison -v
```

## Test Database Credentials

The test containers use the following credentials:

| Database   | Host      | Port | User     | Password | Database | Schema |
|------------|-----------|------|----------|----------|----------|----------|
| PostgreSQL | localhost | 5433 | test_user| test_pass | test_db  | test     |
| Oracle     | localhost | 1521 | test     | test_pass | test_db  | test     |
| ClickHouse | localhost | 8123 | test_user| test_pass | test     | test     |

## Troubleshooting

### Oracle Connection Issues on macOS
If Oracle tests fail on Apple Silicon Mac:
1. Ensure Colima is running with x86_64 architecture
2. Verify Docker context is set to Colima: `docker context use colima`
3. Restart containers: `docker-compose -f docker/docker-compose.yml down -v && docker-compose -f docker/docker-compose.yml up -d`


```bash
# Run with coverage report
python -m pytest tests --cov=src --cov-report=html -v
```

### Database Health Checks
Check if databases are healthy:
```bash
docker-compose -f docker/docker-compose.yml ps

# Check individual container logs
docker-compose -f docker/docker-compose.yml logs postgres
docker-compose -f docker/docker-compose.yml logs oracle
```
