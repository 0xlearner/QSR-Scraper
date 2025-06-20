FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p data logs configs

# Copy application code
COPY . .

# Ensure configs directory exists and has proper permissions
RUN chmod -R 755 configs

# Expose the API port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "scraper_system.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
