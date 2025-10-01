#!/bin/bash

# Retail Insight Cockpit Deployment Script
# This script automates the deployment of the Retail Insight Cockpit Asset Bundle

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
print_info() {
    echo -e "${BLUE}ℹ INFO: $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ SUCCESS: $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}❌ ERROR: $1${NC}"
}

print_header() {
    echo -e "${BLUE}"
    echo "=============================================="
    echo "$1"
    echo "=============================================="
    echo -e "${NC}"
}

# Default values
TARGET_ENV="dev"
CATALOG_NAME=""
SCHEMA_NAME="cockpit"
WORKSPACE_URL=""
SKIP_DATA_GEN="false"
CLEANUP="false"
DRY_RUN="false"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            TARGET_ENV="$2"
            shift 2
            ;;
        -c|--catalog)
            CATALOG_NAME="$2"
            shift 2
            ;;
        -s|--schema)
            SCHEMA_NAME="$2"
            shift 2
            ;;
        -w|--workspace)
            WORKSPACE_URL="$2"
            shift 2
            ;;
        --skip-data)
            SKIP_DATA_GEN="true"
            shift
            ;;
        --cleanup)
            CLEANUP="true"
            shift
            ;;
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        -h|--help)
            echo "Retail Insight Cockpit Deployment Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -e, --env ENVIRONMENT       Target environment (dev/staging/prod) [default: dev]"
            echo "  -c, --catalog CATALOG       Unity Catalog name [required]"
            echo "  -s, --schema SCHEMA          Schema name [default: cockpit]"
            echo "  -w, --workspace URL          Workspace URL [required]"
            echo "  --skip-data                  Skip sample data generation"
            echo "  --cleanup                    Clean up deployment (dev/staging only)"
            echo "  --dry-run                    Show what would be deployed without executing"
            echo "  -h, --help                   Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --env dev --catalog retail_dev --workspace https://myworkspace.databricks.com"
            echo "  $0 --env prod --catalog retail_prod --workspace https://prod.databricks.com --skip-data"
            echo "  $0 --cleanup --env dev --catalog retail_dev"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$CATALOG_NAME" ]]; then
    print_error "Catalog name is required. Use --catalog option."
    exit 1
fi

if [[ -z "$WORKSPACE_URL" ]] && [[ "$CLEANUP" != "true" ]]; then
    print_error "Workspace URL is required for deployment. Use --workspace option."
    exit 1
fi

# Set catalog name based on environment if not fully specified
if [[ "$CATALOG_NAME" != *"_${TARGET_ENV}" ]] && [[ "$TARGET_ENV" != "prod" ]]; then
    CATALOG_NAME="${CATALOG_NAME}_${TARGET_ENV}"
fi

# Display configuration
print_header "RETAIL INSIGHT COCKPIT DEPLOYMENT"
print_info "Environment: $TARGET_ENV"
print_info "Catalog: $CATALOG_NAME"
print_info "Schema: $SCHEMA_NAME"
if [[ -n "$WORKSPACE_URL" ]]; then
    print_info "Workspace: $WORKSPACE_URL"
fi
print_info "Skip data generation: $SKIP_DATA_GEN"
print_info "Cleanup mode: $CLEANUP"
print_info "Dry run: $DRY_RUN"
echo ""

# Dry run mode
if [[ "$DRY_RUN" == "true" ]]; then
    print_info "DRY RUN MODE - No changes will be made"
    print_info "Would execute the following steps:"
    echo "1. Validate Databricks CLI authentication"
    echo "2. Check Unity Catalog access"
    echo "3. Deploy Asset Bundle with target: $TARGET_ENV"
    echo "4. Create tables and sample data"
    echo "5. Setup analytical views"
    echo "6. Configure Genie integration"
    echo "7. Create user groups and permissions"
    echo "8. Validate deployment"
    exit 0
fi

# Cleanup mode
if [[ "$CLEANUP" == "true" ]]; then
    if [[ "$TARGET_ENV" == "prod" ]]; then
        print_error "Cannot cleanup production environment for safety"
        exit 1
    fi

    print_header "CLEANUP MODE"
    print_warning "This will remove the Retail Insight Cockpit deployment"
    echo "Environment: $TARGET_ENV"
    echo "Catalog: $CATALOG_NAME"
    echo "Schema: $SCHEMA_NAME"
    echo ""
    read -p "Are you sure you want to cleanup this deployment? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Cleanup cancelled"
        exit 0
    fi

    print_info "Starting cleanup..."

    # Run cleanup using Databricks notebook
    databricks workspace import --overwrite ./src/utils/deployment_utils.py /tmp/deployment_utils

    databricks jobs create --json '{
        "name": "retail-cockpit-cleanup-'$TARGET_ENV'",
        "notebook_task": {
            "notebook_path": "/tmp/deployment_utils",
            "base_parameters": {
                "catalog_name": "'$CATALOG_NAME'",
                "schema_name": "'$SCHEMA_NAME'",
                "action": "cleanup"
            }
        },
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1
        }
    }' > /tmp/cleanup_job.json

    JOB_ID=$(cat /tmp/cleanup_job.json | jq -r '.job_id')
    databricks jobs run-now --job-id $JOB_ID

    print_success "Cleanup initiated. Check job status in Databricks workspace."
    exit 0
fi

# Main deployment process
print_header "STARTING DEPLOYMENT"

# Step 1: Validate Databricks CLI
print_info "Step 1: Validating Databricks CLI..."
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI not found. Please install it first:"
    echo "pip install databricks-cli"
    exit 1
fi

# Check authentication
if ! databricks workspace list &> /dev/null; then
    print_error "Databricks CLI not authenticated. Please run:"
    echo "databricks configure --token"
    exit 1
fi

print_success "Databricks CLI is configured"

# Step 2: Validate Unity Catalog access
print_info "Step 2: Validating Unity Catalog access..."

# Create a temporary SQL file to test catalog access
cat > /tmp/test_catalog.sql << EOF
USE CATALOG $CATALOG_NAME;
CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME COMMENT 'Retail Insight Cockpit schema';
EOF

if databricks sql execute --file /tmp/test_catalog.sql &> /dev/null; then
    print_success "Unity Catalog access validated"
else
    print_warning "Could not access catalog $CATALOG_NAME. Will attempt to create it during deployment."
fi

rm -f /tmp/test_catalog.sql

# Step 3: Deploy Asset Bundle
print_info "Step 3: Deploying Databricks Asset Bundle..."

# Update databricks.yml with current parameters
cat > databricks.yml << EOF
bundle:
  name: retail-insight-cockpit
  description: "Plug-and-play retail analytics solution for Databricks Lakehouse"

variables:
  catalog_name:
    default: "$CATALOG_NAME"
  schema_name:
    default: "$SCHEMA_NAME"
  workspace_url:
    default: "$WORKSPACE_URL"
  deployment_env:
    default: "$TARGET_ENV"

targets:
  dev:
    mode: development
    variables:
      catalog_name: "$CATALOG_NAME"
  staging:
    mode: production
    variables:
      catalog_name: "$CATALOG_NAME"
  prod:
    mode: production
    variables:
      catalog_name: "$CATALOG_NAME"

resources:
  jobs:
    setup_retail_data:
      name: "retail-cockpit-setup-$TARGET_ENV"
      job_clusters:
        - job_cluster_key: "main"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
      tasks:
        - task_key: "setup_tables"
          job_cluster_key: "main"
          notebook_task:
            notebook_path: "./src/setup/01_create_tables"
            base_parameters:
              catalog_name: "$CATALOG_NAME"
              schema_name: "$SCHEMA_NAME"
EOF

if databricks bundle deploy --target $TARGET_ENV; then
    print_success "Asset Bundle deployed successfully"
else
    print_error "Asset Bundle deployment failed"
    exit 1
fi

# Step 4: Setup data sources
print_info "Step 4: Setting up data sources..."

# Upload notebooks to workspace
databricks workspace import --overwrite ./src/setup/01_create_tables.py /retail-cockpit/setup/01_create_tables
databricks workspace import --overwrite ./src/setup/02_generate_sample_data.py /retail-cockpit/setup/02_generate_sample_data
databricks workspace import --overwrite ./src/setup/03_create_analytical_views.py /retail-cockpit/setup/03_create_analytical_views

# Run table creation
print_info "Creating tables..."
SETUP_JOB_ID=$(databricks jobs list --output json | jq -r '.jobs[] | select(.settings.name | contains("retail-cockpit-setup")) | .job_id')

if [[ -n "$SETUP_JOB_ID" ]]; then
    databricks jobs run-now --job-id $SETUP_JOB_ID --notebook-params '{
        "catalog_name": "'$CATALOG_NAME'",
        "schema_name": "'$SCHEMA_NAME'"
    }'
    print_success "Data setup job started. Job ID: $SETUP_JOB_ID"
else
    print_warning "Setup job not found. Creating and running manually..."

    # Create and run setup job manually
    cat > /tmp/setup_job.json << EOF
{
    "name": "retail-cockpit-setup-manual-$TARGET_ENV",
    "notebook_task": {
        "notebook_path": "/retail-cockpit/setup/01_create_tables",
        "base_parameters": {
            "catalog_name": "$CATALOG_NAME",
            "schema_name": "$SCHEMA_NAME"
        }
    },
    "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
    }
}
EOF

    MANUAL_JOB_ID=$(databricks jobs create --json-file /tmp/setup_job.json | jq -r '.job_id')
    databricks jobs run-now --job-id $MANUAL_JOB_ID
    print_success "Manual setup job created and started. Job ID: $MANUAL_JOB_ID"
fi

# Step 5: Setup Genie integration
print_info "Step 5: Setting up Genie integration..."

# Upload Genie notebooks
databricks workspace import --overwrite ./genie/retail_prompt_library.py /retail-cockpit/genie/retail_prompt_library
databricks workspace import --overwrite ./genie/genie_integration_setup.py /retail-cockpit/genie/genie_integration_setup

print_success "Genie notebooks uploaded"

# Step 6: Create deployment validation script
print_info "Step 6: Creating deployment validation..."

cat > /tmp/validate_deployment.py << 'EOF'
# Validation script
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

catalog_name = sys.argv[1]
schema_name = sys.argv[2]

try:
    # Test table access
    tables = ['sales', 'customers', 'products', 'stores']
    for table in tables:
        count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table}").collect()[0]['count']
        print(f"✅ {table}: {count:,} rows")

    print("🎉 Deployment validation successful!")

except Exception as e:
    print(f"❌ Validation failed: {e}")
    sys.exit(1)
EOF

databricks workspace import --overwrite /tmp/validate_deployment.py /retail-cockpit/validate_deployment

# Step 7: Final status
print_header "DEPLOYMENT SUMMARY"

print_success "Retail Insight Cockpit deployment initiated successfully!"
echo ""
print_info "Next steps:"
echo "1. Monitor job execution in Databricks workspace"
echo "2. Wait for data setup and sample data generation to complete"
echo "3. Access dashboards once jobs finish"
echo "4. Configure user permissions for each role"
echo "5. Test Genie natural language queries"
echo ""

print_info "Quick links:"
echo "• Workspace Jobs: $WORKSPACE_URL/#job/list"
echo "• Catalog: $WORKSPACE_URL/#catalog/$CATALOG_NAME"
echo "• Schema: $WORKSPACE_URL/#catalog/$CATALOG_NAME/$SCHEMA_NAME"
echo ""

print_info "To check deployment status:"
echo "databricks workspace export /retail-cockpit/validate_deployment --format SOURCE"
echo "python validate_deployment.py $CATALOG_NAME $SCHEMA_NAME"
echo ""

print_success "Deployment script completed! 🚀"