# Harness image: Python 3.14 with the dependencies from uv.lock.
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/

ENV UV_PYTHON_DOWNLOADS=never PATH="/harness/.venv/bin:$PATH" PYTHONUNBUFFERED=1

WORKDIR /harness

COPY pyproject.toml uv.lock .python-version ./

RUN uv sync --frozen --no-dev

COPY *.py ./
