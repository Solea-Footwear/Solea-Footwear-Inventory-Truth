# Pin to specific patch version to avoid surprise minor-version updates
FROM python:3.11.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install system dependencies:
#   gcc/g++/postgres-client  — psycopg2 build deps
#   chromium + chromium-driver — required by Mercari/Poshmark Selenium listers
#   curl                     — used by docker-compose app healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        postgresql-client \
        postgresql-contrib \
        chromium \
        chromium-driver \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Create non-root user and hand over /app
RUN useradd --create-home --uid 1001 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Selenium uses the system Chromium; tell webdriver-manager / chromedriver where
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_BIN=/usr/bin/chromedriver \
    SELENIUM_HEADLESS=true \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Expose the port that the application runs on
EXPOSE 9500

# Define the command to run the application
CMD ["python", "-m", "src.backend.app"]
