Write-Host "=== ROSSMANN REPLICATION TEST SCRIPT (POWERSHELL) ===" -ForegroundColor Cyan

Write-Host "1. Cleaning up existing containers and volumes (Hard Reset)..." -ForegroundColor Yellow
docker-compose down -v

Write-Host "2. Starting containers in background..." -ForegroundColor Yellow
docker-compose up -d --build

Write-Host "3. Waiting for PostgreSQL and replica set to initialize (12 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 12

Write-Host "4. Checking if receipts were successfully replicated (initial run)..." -ForegroundColor Yellow
Write-Host "--- Receipts count in PostgreSQL ---" -ForegroundColor Green
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT COUNT(*) FROM receipts;"

Write-Host "5. Querying receipts containing 'Żel' products..." -ForegroundColor Yellow
Write-Host "--- JSONB Query Results ---" -ForegroundColor Green
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "
SELECT DISTINCT 
    r.transaction_id, 
    r.data->'customer'->>'first_name' AS client_name,
    (r.data->'payment'->>'amount_paid')::numeric AS amount
FROM receipts r,
LATERAL jsonb_to_recordset(r.data->'items') AS item(name TEXT)
WHERE item.name LIKE 'Żel%';
"

Write-Host "6. Triggering generator container to add 10 MORE receipts..." -ForegroundColor Yellow
docker-compose start python-app

Write-Host "Waiting for generator to finish (5 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "7. Checking receipts count again (should increase by 10)..." -ForegroundColor Yellow
Write-Host "--- Updated Receipts count in PostgreSQL ---" -ForegroundColor Green
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT COUNT(*) FROM receipts;"

Write-Host "8. Checking current replication offset state..." -ForegroundColor Yellow
Write-Host "--- Replication State ---" -ForegroundColor Green
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT * FROM replication_state;"

Write-Host "=== TEST COMPLETED SUCCESSFULLY! ===" -ForegroundColor Cyan
