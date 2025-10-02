#!/bin/bash

# Dashboard Deployment Script for Retail Insight Cockpit
# This script imports dashboard JSON files into Databricks Lakeview

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
CATALOG_NAME=${1:-retail_demo}
SCHEMA_NAME=${2:-cockpit}
WAREHOUSE_NAME=${3:-retail-cockpit-dev}

print_status "Deploying Retail Insight Cockpit Dashboards"
echo "Catalog: $CATALOG_NAME"
echo "Schema: $SCHEMA_NAME"
echo "Warehouse: $WAREHOUSE_NAME"
echo ""

# Check if dashboards directory exists
if [ ! -d "dashboards" ]; then
    print_error "Dashboards directory not found"
    exit 1
fi

# Get warehouse ID
print_status "Getting SQL Warehouse ID..."
WAREHOUSE_ID=$(databricks sql warehouses list --output json | jq -r ".warehouses[] | select(.name == \"$WAREHOUSE_NAME\") | .id")

if [ -z "$WAREHOUSE_ID" ] || [ "$WAREHOUSE_ID" = "null" ]; then
    print_error "SQL Warehouse '$WAREHOUSE_NAME' not found"
    print_status "Available warehouses:"
    databricks sql warehouses list --output table
    exit 1
fi

print_success "Found warehouse ID: $WAREHOUSE_ID"

# Function to import dashboard
import_dashboard() {
    local dashboard_file=$1
    local dashboard_name=$2

    print_status "Importing $dashboard_name..."

    # Update the JSON with correct warehouse ID and schema references
    temp_file=$(mktemp)
    sed "s/{{ warehouse_id }}/$WAREHOUSE_ID/g; s/retail_demo\\.cockpit/$CATALOG_NAME.$SCHEMA_NAME/g" "$dashboard_file" > "$temp_file"

    # Import dashboard using Databricks CLI
    if databricks lakeview import "$temp_file"; then
        print_success "Successfully imported $dashboard_name"
    else
        print_error "Failed to import $dashboard_name"
    fi

    rm "$temp_file"
}

# Import all dashboards
print_status "Starting dashboard import process..."

# Store Manager Dashboard
if [ -f "dashboards/store_manager_dashboard.json" ]; then
    import_dashboard "dashboards/store_manager_dashboard.json" "Store Manager Cockpit"
else
    print_error "Store Manager dashboard file not found"
fi

# Merchandiser Dashboard
if [ -f "dashboards/merchandiser_dashboard.json" ]; then
    import_dashboard "dashboards/merchandiser_dashboard.json" "Merchandiser Analytics"
else
    print_error "Merchandiser dashboard file not found"
fi

# Supply Chain Dashboard
if [ -f "dashboards/supply_chain_dashboard.json" ]; then
    import_dashboard "dashboards/supply_chain_dashboard.json" "Supply Chain Insights"
else
    print_error "Supply Chain dashboard file not found"
fi

# Executive Dashboard
if [ -f "dashboards/executive_dashboard.json" ]; then
    import_dashboard "dashboards/executive_dashboard.json" "Executive Summary"
else
    print_error "Executive dashboard file not found"
fi

print_success "Dashboard deployment completed!"
echo ""
print_status "Next steps:"
echo "1. Go to Databricks workspace > Dashboards"
echo "2. Find your imported dashboards"
echo "3. Configure any additional parameters"
echo "4. Share with appropriate user groups"
echo ""
print_status "Dashboard URLs will be available in your Databricks workspace under 'Dashboards'"