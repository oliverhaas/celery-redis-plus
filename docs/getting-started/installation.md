# Installation

## Requirements

- Python >= 3.13
- Celery >= 5.5.0
- Redis >= 7.0 (for BZMPOP) or Valkey (any version)

## Install with uv

```bash
uv add celery-redis-plus
```

## Install with pip

```bash
pip install celery-redis-plus
```

## Development Installation

```bash
# Clone the repository
git clone https://github.com/oliverhaas/celery-redis-plus.git
cd celery-redis-plus

# Create virtual environment and install with development dependencies
uv venv
uv sync --group dev

# Run tests (requires Docker for integration tests)
uv run pytest

# Run linter
uv run ruff check

# Run type checker
uv run ty check
```
