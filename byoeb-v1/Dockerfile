# Use an official Python runtime
FROM python:3.11-slim-bullseye

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_VIRTUALENVS_CREATE=false

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg curl build-essential git && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install "poetry==$POETRY_VERSION"

# Set workdir
WORKDIR /app

# Copy dependency manifests and local path packages first for better caching
COPY byoeb ./byoeb
COPY byoeb/pyproject.toml ./byoeb/
COPY byoeb/poetry.lock* ./byoeb/

COPY byoeb-integrations ./byoeb-integrations
COPY byoeb-integrations/pyproject.toml ./byoeb-integrations/
COPY byoeb-integrations/poetry.lock* ./byoeb-integrations/

COPY byoeb-core ./byoeb-core
COPY byoeb-core/pyproject.toml ./byoeb-core/
COPY byoeb-core/poetry.lock* ./byoeb-core/

# Change directory to service's root and install dependencies via Poetry
WORKDIR /app/byoeb
RUN poetry lock --no-interaction --no-ansi && poetry install --no-root --no-interaction --no-ansi

# Copy the full project after dependencies are cached
WORKDIR /app
COPY . .
WORKDIR /app/byoeb

# Expose the port (Render expects 10000 internally, but you can set 5000)
EXPOSE 8000

# Command to run your FastAPI app
CMD ["poetry", "run", "python3", "-m", "byoeb.chat_app.run", "--host", "0.0.0.0", "--port", "8000"]
