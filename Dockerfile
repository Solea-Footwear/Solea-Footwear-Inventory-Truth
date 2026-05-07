# Use official Python runtime as a base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install system dependencies for psycopg2 and other packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the application runs on
EXPOSE 9500

# Define the command to run the application
CMD ["python", "app.py"]