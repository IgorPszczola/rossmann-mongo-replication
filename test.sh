#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== ROSSMANN REPLICATION TEST SCRIPT ==="

echo "1. Cleaning up existing containers and volumes (Hard Reset)..."
docker-compose down -v

echo "2. Starting containers in background..."
docker-compose up -d --build

echo "3. Waiting for PostgreSQL and replica set to initialize (12 seconds)..."
sleep 12

echo "4. Checking if receipts were successfully replicated (initial run)..."
echo "--- Receipts count in PostgreSQL ---"
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT COUNT(*) FROM receipts;"

echo "5. Querying receipts containing 'Żel' products..."
echo "--- JSONB Query Results ---"
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "
SELECT DISTINCT 
    r.transaction_id, 
    r.data->'customer'->>'first_name' AS client_name,
    (r.data->'payment'->>'amount_paid')::numeric AS amount
FROM receipts r,
LATERAL jsonb_to_recordset(r.data->'items') AS item(name TEXT)
WHERE item.name LIKE 'Żel%';
"

echo "6. Triggering generator container to add 10 MORE receipts..."
docker-compose start python-app

echo "Waiting for generator to finish (5 seconds)..."
sleep 5

echo "7. Checking receipts count again (should increase by 10)..."
echo "--- Updated Receipts count in PostgreSQL ---"
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT COUNT(*) FROM receipts;"

echo "8. Checking current replication offset state..."
echo "--- Replication State ---"
docker exec rossmann_postgres psql -U rossmann_user -d rossmann_relational_db -c "SELECT * FROM replication_state;"

echo "=== TEST COMPLETED SUCCESSFULLY! ==="
