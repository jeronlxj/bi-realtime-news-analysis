# PowerShell script for testing Spark setup on Windows
Write-Host "=== Spark Version Compatibility Fix ===" -ForegroundColor Green
Write-Host "Fixed PySpark version mismatch: Updated to use Spark 3.3.0 consistently" -ForegroundColor Yellow
Write-Host ""

# Stop any running containers first
Write-Host "Stopping any existing containers..." -ForegroundColor Cyan
docker-compose down --remove-orphans

# Clean up old images to ensure we get the right versions
Write-Host "Cleaning up old Spark images..." -ForegroundColor Cyan
docker rmi bitnami/spark:3 -f 2>$null
docker rmi bitnami/spark:latest -f 2>$null

# Build and start containers with the fixed versions
Write-Host "Building and starting containers with Spark 3.3.0..." -ForegroundColor Cyan
docker-compose up --build -d

Write-Host "Waiting for services to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 45  # Give more time for Spark to start properly

Write-Host "Checking service status..." -ForegroundColor Cyan
docker-compose ps

Write-Host ""
Write-Host "Checking Spark Master version..." -ForegroundColor Cyan
docker-compose logs spark | Select-String "Running Spark version"

Write-Host ""
Write-Host "Testing Spark connection from backend container..." -ForegroundColor Cyan
docker-compose exec backend python src/test_spark.py

Write-Host ""
Write-Host "Viewing recent backend logs..." -ForegroundColor Cyan
docker-compose logs backend | Select-Object -Last 15

Write-Host ""
Write-Host "=== Quick Verification Steps ===" -ForegroundColor Green
Write-Host "1. Spark Master UI: http://localhost:8080" -ForegroundColor White
Write-Host "2. Check for version compatibility errors in logs" -ForegroundColor White
Write-Host "3. Verify worker is connected to master" -ForegroundColor White

Write-Host ""
Write-Host "If you still see serialVersionUID errors:" -ForegroundColor Yellow
Write-Host "  - Run: docker-compose restart backend" -ForegroundColor White
Write-Host "  - Check: docker-compose logs spark-worker" -ForegroundColor White
