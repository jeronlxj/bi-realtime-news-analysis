#!/bin/bash
# Docker container startup script for testing Spark connectivity

echo "Starting Docker containers for bi-realtime-news-analysis..."
echo "This will help diagnose and fix the SparkContext issues"

# Build and start containers
echo "Building and starting containers..."
docker-compose up --build -d

echo "Waiting for services to start..."
sleep 30

echo "Checking service status..."
docker-compose ps

echo ""
echo "Testing Spark connection from backend container..."
docker-compose exec backend python src/test_spark.py

echo ""
echo "Viewing backend logs for Spark initialization..."
docker-compose logs backend | tail -20

echo ""
echo "If you see Spark errors, try restarting just the Spark services:"
echo "docker-compose restart spark spark-worker"

echo ""
echo "To view all logs: docker-compose logs"
echo "To stop all services: docker-compose down"
