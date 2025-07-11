#!/bin/bash

# PandaETL Launch Script with Nginx Reverse Proxy
# This script sets up and launches the PandaETL application

echo "🐼 Starting PandaETL with Nginx reverse proxy..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker is not running. Please start Docker and try again."
  exit 1
fi

# Stop any existing containers
echo "🧹 Cleaning up existing containers..."
docker compose down

# Remove existing database if requested
if [ "$1" == "--fresh" ]; then
  echo "🗑️  Removing existing database for a fresh start..."
  rm -f instance/app.db
fi

# Build the containers
echo "🏗️  Building containers..."
docker compose build

# Start the application with Nginx reverse proxy
echo "🚀 Starting PandaETL application..."
docker compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 5

# Check if services are running
if docker ps | grep -q "panda-etl-nginx"; then
  echo "✅ PandaETL is now running!"
  echo "🌐 Access the application at: http://localhost:8080"
  echo ""
  echo "📋 Usage instructions:"
  echo "  1. Go to http://localhost:8080 in your browser"
  echo "  2. Request an API key with your email"
  echo "  3. Create a new project and start using PandaETL"
  echo ""
  echo "⚠️  To stop the application, run: docker compose down"
  echo "🔄 To restart with a fresh database, run: ./launch.sh --fresh"
else
  echo "❌ Something went wrong. Check docker logs for more information:"
  echo "   docker logs panda-etl-nginx"
  echo "   docker logs panda-etl-frontend"
  echo "   docker logs panda-etl-backend"
fi
