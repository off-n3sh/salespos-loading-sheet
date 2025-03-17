# Use a lightweight Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/salespos-578ff-firebase-adminsdk-fbsvc-e3d51aa7c5.json
ENV FLASK_SECRET_KEY=5845f7ddf6ceb74f2e99c885af1e500c

# Copy requirements file
COPY requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

# Copy the rest of the application
COPY . .

# Run the app with gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
