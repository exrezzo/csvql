#!/bin/bash
# Demo script for CSVQL tool
# Run this after building with: go build -o csvql ./cmd/csvql

set -e

cd "$(dirname "$0")"

echo "=== CSVQL Demo ==="
echo ""
echo "Building csvql..."
go build -o csvql ./cmd/csvql

echo ""
echo "=== Starting interactive mode with testdata ==="
echo ""
echo "Example queries to try:"
echo ""
echo "  .tables                                    # List all tables"
echo "  .schema employees                          # Show columns"
echo "  SELECT * FROM employees LIMIT 5;           # Basic query"
echo "  SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal"
echo "    FROM employees GROUP BY department;      # Aggregation"
echo "  SELECT e.name, o.product, o.amount"
echo "    FROM employees e"
echo "    JOIN sales_orders o ON e.id = o.employee_id"
echo "    ORDER BY o.amount DESC LIMIT 5;          # Join query"
echo "  SELECT * FROM inventory_products;          # TSV file"
echo ""
echo "Type .quit to exit"
echo ""

./csvql -dir ./testdata

# Cleanup
rm -f csvql
