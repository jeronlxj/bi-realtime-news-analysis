# Spark Context Issue - Troubleshooting Guide

## Problem SOLVED ✅
The original SparkContext error was caused by a **version incompatibility** between:
- **Spark Master**: version 3.3.0 (Docker container)
- **PySpark Client**: version 4.0.0 (Python requirements)

The error `java.io.InvalidClassException` with `serialVersionUID` mismatch indicates incompatible serialization between different Spark versions.

## Root Causes
1. **Version Mismatch** ⚠️: Most common cause - different Spark versions between master and client
2. **Multiple SparkSession creation attempts**: Code tries to create SparkSession without properly cleaning up existing contexts
3. **Resource conflicts**: Insufficient memory or CPU resources for Spark containers
4. **Network connectivity**: Backend cannot connect to Spark master
5. **Container startup timing**: Backend starts before Spark master is fully ready

## ✅ SOLUTION APPLIED

### 1. Fixed Version Compatibility
- Updated `requirements.txt`: `pyspark==3.3.0` (was 4.0.0)
- Updated `docker-compose.yml`: `bitnami/spark:3.3.0` (was bitnami/spark:3)
- Both Spark master and PySpark client now use the same version

### 2. Enhanced Docker Configuration
- Added health checks for Spark master
- Improved service dependencies with condition checks
- Added proper resource allocation for Spark containers
- Enhanced networking configuration

### 3. Spark Session Management
- Created `SparkManager` utility class for proper session lifecycle management
- Added connection retry logic with exponential backoff
- Implemented session health checks and automatic restart capabilities
- Added proper cleanup mechanisms

### 4. Service Initialization
- Added service readiness checks for Kafka, PostgreSQL, and Spark
- Implemented graceful error handling when Spark is unavailable
- Added connection testing before creating SparkSession

## Quick Fix Steps

### Step 1: Test Current Setup
```bash
# Run the PowerShell test script
.\test_spark_setup.ps1

# Or manually check containers
docker-compose ps
docker-compose logs spark
docker-compose logs backend
```

### Step 2: Restart Spark Services
```bash
# If Spark services are having issues
docker-compose restart spark spark-worker

# Wait a moment then test
docker-compose exec backend python src/test_spark.py
```

### Step 3: Clean Restart (if needed)
```bash
# Stop everything
docker-compose down --remove-orphans

# Clean up volumes if needed
docker volume prune

# Restart
docker-compose up --build
```

## Verification Steps

### 1. Check Spark Master UI
- Open http://localhost:8080 in your browser
- Verify the master is running and worker is connected

### 2. Test Spark Connection
```bash
# From inside the backend container
docker-compose exec backend python src/test_spark.py
```

### 3. Check API Endpoints
```bash
# Test the backend API
curl http://localhost:8000/api/news

# Test analysis endpoint (requires Spark)
curl http://localhost:8000/api/analyze?query=test
```

## Configuration Changes Made

### Docker Compose Updates
- Added health checks for Spark master
- Improved resource allocation
- Added dependency conditions
- Enhanced volume management

### Code Improvements
- `SparkManager` class for session management
- Retry logic and connection testing
- Proper cleanup mechanisms
- Error handling improvements

## Monitoring

### Key Log Messages to Watch For
- ✅ "Successfully connected to Spark master"
- ✅ "SparkSession created successfully"
- ❌ "Cannot connect to Spark master"
- ❌ "SparkContext has been shut down"

### Performance Monitoring
- Monitor memory usage: `docker stats`
- Check container logs: `docker-compose logs -f`
- Spark UI metrics: http://localhost:8080

## Common Issues and Solutions

### Issue: "No active SparkContext"
**Solution**: Restart backend container
```bash
docker-compose restart backend
```

### Issue: "Connection refused to Spark master"
**Solution**: Check Spark master health and restart if needed
```bash
docker-compose restart spark spark-worker
```

### Issue: "Out of memory" errors
**Solution**: Increase Docker memory allocation or reduce Spark memory configs

### Issue: Containers keep restarting
**Solution**: Check logs and ensure all dependencies are healthy
```bash
docker-compose logs --tail=50
```

## Best Practices for Future Development

1. **Always test Spark connectivity** before using SparkSession
2. **Implement proper error handling** for Spark operations
3. **Use the SparkManager** for session lifecycle management
4. **Monitor resource usage** regularly
5. **Test with realistic data volumes** to ensure stability

This setup now provides robust Spark session management with proper error handling and recovery mechanisms.
