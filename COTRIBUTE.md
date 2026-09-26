# Contributing to xoverrr

Thank you for considering a contribution to xoverrr, a tool for cross-database data comparison and quality validation. This document sets out the standards and processes that keep contributions consistent with the project's integrity, clarity, and reliability.

## Philosophy and code quality

xoverrr is built with an emphasis on **precision, efficiency, and maintainability**. Contributions should reflect these principles:

*   **Clarity over cleverness:** write code that is immediately understandable to other engineers. Avoid unnecessary complexity or obscure optimisations that sacrifice readability.
*   **Robustness over speed:** while performance is important, correctness and resilience (proper error handling and consideration of edge cases) are paramount.
*   **Explicit over implicit:** prefer clear, descriptive naming and explicit logic to terse, implicit patterns that may be ambiguous.

Contributions that compromise these tenets in favour of personal style or tangled code will be declined during review.

## Development workflow

### 1. Environment setup

Follow the instructions in `tests/README.md` to create a virtual environment and install the development dependencies. Ensure that all unit tests pass before you make changes.

### 2. Branching strategy

*   Create a feature branch from `main`: `git checkout -b fix/issue-123_short_description`.

### 3. Making changes

*   **Scope:** keep changes focused and atomic. A pull request should address a single issue or feature.
*   **Code style:** follow the existing codebase style. Use `ruff` and `isort` (configured as development dependencies) to format your code automatically.
*   **Type hints:** all new functions, and any significant modifications, must include comprehensive Python type hints.
*   **Documentation:** update docstrings, comments, and the relevant documentation (`README.md`) to reflect your changes. Docstrings should follow the existing project convention.
*   **Tests:** add or update unit and integration tests in `tests/` for all new functionality.

### 4. Pre-commit validation

Before you submit, run the following checks from the project root:

```bash
# Format code
ruff format src/ tests/
isort src/ tests/

# Run static type checking (optional but recommended)
mypy src/

# Execute the test suite
pytest tests/unit -v
```

### 5. Submitting a pull request

* **Title:** use a clear, imperative title (for example, "Fix timestamp conversion in the Oracle adapter").
* **Description:** provide a concise summary of the changes, the problem solved, and any relevant context. Link to related issues.
* **Review:** request a review. Be prepared to discuss the changes and to make iterative improvements in response to feedback.

GitHub Actions runs unit tests on every push to a branch.

**Run workflow** is a GitHub UI control that only appears after the workflow file exists on `main`. Until then it will not show on a feature branch. To run Docker integration tests from this branch, push a commit whose message contains `[integration]`:

```bash
git commit --allow-empty -m "Run integration tests [integration]"
git push
```

After merge to `main`: **Actions** -> left sidebar **CI** or **Integration tests** -> **Run workflow** on the right. That button is not on an existing run page (there you only get **Re-run**).

## Releasing

Publish only from `main`, with a version tag that matches both `pyproject.toml` and `src/xoverrr/version.py`.

1. Merge the work into `main`.
2. Set the same version in `pyproject.toml` and `src/xoverrr/version.py` (for example `1.4.6`).
3. Create and push an annotated tag:

```bash
git checkout main
git pull
git tag -a vX.X.X -m "vX.X.X"
git push origin vX.X.X
```

The **Release** workflow then runs unit tests, Docker integration tests, builds the package, and publishes to PyPI via Trusted Publishing.

The same `Makefile` targets (`test-unit`, `test-integration`, `build`) are what CI runs. If the project later moves off GitHub, those targets can be called from GitLab CI or any other runner without changing how tests are invoked.

One-time PyPI setup (repository owner):

1. In GitHub: **Settings -> Environments -> New environment** named `pypi` (optional: require a reviewer).
2. In PyPI: **Publishing -> Add a new pending publisher** (or add a publisher on the existing `xoverrr` project):
   * Owner: `dima-ischenko`
   * Repository: `xoverrr`
   * Workflow: `release.yml`
   * Environment: `pypi`
