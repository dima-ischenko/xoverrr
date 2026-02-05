# Contributing to xoverrr

Thank you for considering contributing to xoverrr, a sophisticated tool for cross-database data comparison and quality validation. This document outlines the standards and processes that ensure contributions maintain the project's integrity, clarity, and reliability.

## Philosophy & Code Quality

xoverrr is built with an emphasis on **precision, efficiency, and maintainability**. Contributions should reflect these core principles:

*   **Clarity over Cleverness:** Write code that is immediately understandable to other engineers. Avoid unnecessary complexity or obscure optimizations that sacrifice readability.
*   **Robustness over Speed:** While performance is critical, correctness and resilience (proper error handling, edge-case consideration) are paramount.
*   **Explicit over Implicit:** Prefer clear, verbose naming and explicit logic over terse, implicit patterns that may be ambiguous.

Contributions that compromise these tenets in favor of personal style or "spaghetti code" will be respectfully declined during review.

## Development Workflow

### 1. Environment Setup
Follow the precise instructions in `tests/README.md` to create a virtual environment and install development dependencies. Ensure all unit tests pass before making changes.

### 2. Branching Strategy
*   Create a feature branch from `main`: `git checkout -b fix/issue-123_short_description`.

### 3. Making Changes
*   **Scope:** Keep changes focused and atomic. A pull request should address a single issue or feature.
*   **Code Style:** Adhere to the existing codebase style. Use `ruff` and `isort` (configured as dev dependencies) to format your code automatically.
*   **Type Hints:** All new functions and significant modifications must include comprehensive Python type hints.
*   **Documentation:** Update docstrings, comments, and relevant documentation (`README.md`) to reflect your changes. Docstrings should follow the existing project convention.
*   **Tests:** Add or update unit/integrations tests in `tests/` for all new functionality. 

### 4. Pre-commit Validation
Before submitting, run the following checks from the project root:
```bash
# Format code
ruff format src/ tests/
isort src/ tests/

# Run static type checking (optional but recommended)
mypy src/

# Execute the test suite
pytest tests/unit -v
```
### 5. Submitting a Pull Request

* **Title**: Use a clear, imperative title (e.g., "Fix timestamp conversion in Oracle adapter").
* **Description**: Provide a concise summary of the changes, the problem solved, and any relevant context. Link to related issues.
* **Review**: Request a review. Be prepared to engage in discussion and make iterative improvements based on feedback.
