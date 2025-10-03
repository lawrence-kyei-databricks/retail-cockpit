#!/bin/bash

# Databricks App Deployment Script for Retail Insight Cockpit
# This script packages and deploys the complete retail analytics solution as a Databricks App

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
APP_NAME=${1:-retail-insight-cockpit}
ENVIRONMENT=${2:-dev}

print_status "Deploying Retail Insight Cockpit as Databricks App"
echo "App Name: $APP_NAME"
echo "Environment: $ENVIRONMENT"
echo ""

# Check if Databricks CLI is installed and configured
print_status "Checking Databricks CLI configuration..."
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed"
    echo "Install it with: pip install databricks-cli"
    exit 1
fi

# Check authentication
if ! databricks current-user me &> /dev/null; then
    print_error "Databricks CLI is not configured or authenticated"
    echo "Run: databricks configure --token"
    exit 1
fi

print_success "Databricks CLI is properly configured"

# Validate app structure
print_status "Validating app structure..."
required_files=(
    "app.py"
    "app_requirements.txt"
    "databricks-app.yml"
    "databricks.yml"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        print_error "Required file not found: $file"
        exit 1
    fi
done

print_success "App structure validated"

# Check if app already exists
print_status "Checking if app already exists..."
if databricks apps list --output json | jq -e ".apps[] | select(.name == \"$APP_NAME\")" > /dev/null 2>&1; then
    print_warning "App '$APP_NAME' already exists"
    read -p "Do you want to update it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_status "Deployment cancelled"
        exit 0
    fi
    UPDATE_EXISTING=true
else
    UPDATE_EXISTING=false
fi

# Deploy Asset Bundle first (infrastructure)
print_status "Deploying infrastructure with Databricks Asset Bundle..."
if databricks bundle deploy --target $ENVIRONMENT; then
    print_success "Asset Bundle deployed successfully"
else
    print_error "Asset Bundle deployment failed"
    exit 1
fi

# Create app package
print_status "Creating app package..."
TEMP_DIR=$(mktemp -d)
APP_PACKAGE="$TEMP_DIR/retail-insight-cockpit.zip"

# Copy app files to temp directory
cp -r . "$TEMP_DIR/app_source/"
cd "$TEMP_DIR/app_source/"

# Clean up unnecessary files
rm -rf .git __pycache__ *.pyc .env *.log
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Create zip package
cd "$TEMP_DIR"
zip -r "$APP_PACKAGE" app_source/ > /dev/null

print_success "App package created: $APP_PACKAGE"

# Deploy or update the app
if [ "$UPDATE_EXISTING" = true ]; then
    print_status "Updating existing app..."
    if databricks apps update "$APP_NAME" --source-code-path "$APP_PACKAGE"; then
        print_success "App updated successfully"
    else
        print_error "App update failed"
        exit 1
    fi
else
    print_status "Creating new app..."
    if databricks apps create "$APP_NAME" \
        --description "Complete retail analytics solution with dashboards and Genie AI" \
        --source-code-path "$APP_PACKAGE"; then
        print_success "App created successfully"
    else
        print_error "App creation failed"
        exit 1
    fi
fi

# Wait for app to be ready
print_status "Waiting for app to be ready..."
for i in {1..30}; do
    APP_STATUS=$(databricks apps get "$APP_NAME" --output json | jq -r '.status')
    if [ "$APP_STATUS" = "RUNNING" ]; then
        print_success "App is running!"
        break
    elif [ "$APP_STATUS" = "ERROR" ]; then
        print_error "App failed to start"
        databricks apps get "$APP_NAME"
        exit 1
    else
        echo -n "."
        sleep 10
    fi
done

if [ "$APP_STATUS" != "RUNNING" ]; then
    print_warning "App status: $APP_STATUS (may still be starting)"
fi

# Get app URL
APP_URL=$(databricks apps get "$APP_NAME" --output json | jq -r '.url')

print_success "Retail Insight Cockpit deployed successfully!"
echo ""
echo "App Details:"
echo "   Name: $APP_NAME"
echo "   Status: $APP_STATUS"
echo "   URL: $APP_URL"
echo ""

print_status "Next Steps:"
echo "1. Open the app: $APP_URL"
echo "2. Configure your retail environment through the UI"
echo "3. Deploy your complete retail analytics solution"
echo "4. Access dashboards and Genie AI"
echo ""

# Cleanup
rm -rf "$TEMP_DIR"

print_status "Deployment Summary:"
echo "[OK] Infrastructure deployed (Databricks Asset Bundle)"
echo "[OK] App package created and deployed"
echo "[OK] Streamlit UI available for configuration"
echo "[OK] Integrated Genie AI setup included"
echo "[OK] Role-based dashboards ready"
echo ""

print_success "Your Retail Insight Cockpit is ready!"
echo "Visit: $APP_URL"