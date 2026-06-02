Write-Host "=== Building and starting Live Object Tracker stack ===" -ForegroundColor Cyan

# Build images
docker-compose build

# Start services
docker-compose up -d

Write-Host "`n=== Services started! ===" -ForegroundColor Green
Write-Host "Service: http://localhost:5000" -ForegroundColor White
Write-Host "Postgres: localhost:5432" -ForegroundColor White
Write-Host "`nView logs:" -ForegroundColor Cyan
Write-Host "  docker-compose logs -f service" -ForegroundColor Gray
Write-Host "  docker-compose logs -f postgres" -ForegroundColor Gray
Write-Host "`nStop services:" -ForegroundColor Cyan
Write-Host "  docker-compose down" -ForegroundColor Gray