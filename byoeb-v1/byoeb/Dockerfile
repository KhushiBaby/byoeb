# Use an official Python runtime
FROM python:3.11-slim

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

# Copy project files
COPY . .

# Install dependencies via Poetry
RUN poetry install --no-root --no-interaction --no-ansi

# Expose the port (Render expects 10000 internally, but you can set 5000)
EXPOSE 8000

# Command to run your FastAPI app
CMD ["poetry", "run", "python3", "-m", "byoeb.chat_app.run", "--host", "0.0.0.0", "--port", "8000"]
