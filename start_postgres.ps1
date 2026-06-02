# start_postgres.ps1
# Script to start Postgres in Docker and create the 'coordinates' table
#Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
param(
    [string]$ContainerName = "live-tracker-db",
    [string]$PostgresUser = "postgres",
    [string]$PostgresPassword = "mysecretpassword",
    [string]$PostgresDb = "postgres",
    [int]$Port = 5432
)

Write-Host "=== Live Object Tracker: Postgres Setup ===" -ForegroundColor Cyan

# 1. Check if container already exists
$existingContainer = docker ps -a --filter "name=$ContainerName" --format "{{.Names}}"
if ($existingContainer -eq $ContainerName) {
    Write-Host "[WARN] Container '$ContainerName' already exists. Removing old one..." -ForegroundColor Yellow
    docker rm -f $ContainerName | Out-Null
}

# 2. Start Postgres
Write-Host "[INFO] Starting Postgres 15 in Docker..." -ForegroundColor Green
docker run --name $ContainerName `
    -e POSTGRES_USER=$PostgresUser `
    -e POSTGRES_PASSWORD=$PostgresPassword `
    -e POSTGRES_DB=$PostgresDb `
    -p "${Port}:5432" `
    -d postgres:15 | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to start container!" -ForegroundColor Red
    exit 1
}

# 3. Wait for Postgres to initialize
Write-Host "[INFO] Waiting for Postgres initialization..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 4. Verify container is running
$containerStatus = docker ps --filter "name=$ContainerName" --format "{{.Status}}"
if ($containerStatus -like "Up*") {
    Write-Host "[OK] Postgres is running! Status: $containerStatus" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Container failed to start. Check logs: docker logs $ContainerName" -ForegroundColor Red
    exit 1
}

# 5. Create the 'coordinates' table
Write-Host "[INFO] Creating 'coordinates' table..." -ForegroundColor Cyan
docker exec -i $ContainerName psql -U $PostgresUser -d $PostgresDb -c "CREATE TABLE IF NOT EXISTS coordinates (object_id BIGINT PRIMARY KEY, x REAL, y REAL, z REAL, timestamp BIGINT);"

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Table 'coordinates' created successfully!" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Failed to create table!" -ForegroundColor Red
    exit 1
}

# 6. Print connection info
Write-Host ""
Write-Host "=== Setup Complete! ===" -ForegroundColor Green
Write-Host "C# Connection String:" -ForegroundColor Cyan
Write-Host "Host=localhost;Port=$Port;Username=$PostgresUser;Password=$PostgresPassword;Database=$PostgresDb;Maximum Pool Size=10;" -ForegroundColor White
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
Write-Host "  docker exec -it $ContainerName psql -U $PostgresUser -d $PostgresDb" -ForegroundColor Gray
Write-Host "  docker logs -f $ContainerName" -ForegroundColor Gray
Write-Host "  docker rm -f $ContainerName  (to clean up)" -ForegroundColor Gray