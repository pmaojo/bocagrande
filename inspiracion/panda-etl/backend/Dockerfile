FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry==1.6.1

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt ./

# Install dependencies directly with pip
RUN pip install -r requirements.txt

# Copy poetry files for reference (not used for installation)
COPY pyproject.toml poetry.lock* ./

# Create necessary directories
RUN mkdir -p /app/instance /app/uploads /app/processed /app/exports

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Expose the port the app runs on
EXPOSE 5328

# Command to run the application
CMD ["uvicorn", "app.main:app", "--workers", "2", "--host", "0.0.0.0", "--port", "5328"]
