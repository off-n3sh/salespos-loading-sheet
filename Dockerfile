# Use a lightweight Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Copy requirements file
COPY requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

# Copy the app code (excluding firebase_config.json, since it’s added during build)
COPY . .

# Copy the firebase_config.json (will be provided by GitHub Actions)
COPY firebase_config.json ./firebase_config.json

# Run the app with gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
