#!/bin/bash

# Simple script to create a Google Cloud service account for GCS action runner
# This is a simplified version with minimal options

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🔧 Google Cloud Service Account Creator for GCS Action Runner${NC}"
echo ""

# Get project ID
if [ -z "$1" ]; then
    echo -e "${RED}❌ Project ID is required${NC}"
    echo "Usage: $0 <PROJECT_ID>"
    echo "Example: $0 my-gcp-project-123"
    exit 1
fi

PROJECT_ID="$1"
SERVICE_ACCOUNT_NAME="gcs-action-runner"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "  Output: ./credentials/gcs-credentials.json"
echo ""

# Check gcloud
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ gcloud CLI not found. Please install it first.${NC}"
    echo "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo -e "${RED}❌ Not authenticated. Please run: gcloud auth login${NC}"
    exit 1
fi

# Set project
echo -e "${BLUE}🔧 Setting project...${NC}"
gcloud config set project "$PROJECT_ID"

# Create service account (ignore if exists)
echo -e "${BLUE}🔧 Creating service account...${NC}"
gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
    --display-name="GCS Action Runner" \
    --description="Service account for GCS action runner to process telemetry data" \
    2>/dev/null || echo "Service account already exists"

# Assign roles
echo -e "${BLUE}🔧 Assigning roles...${NC}"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/storage.objectViewer" \
    --quiet

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/storage.objectAdmin" \
    --quiet

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/storage.bucketReader" \
    --quiet

# Create credentials directory
mkdir -p credentials

# Create key
echo -e "${BLUE}🔧 Creating service account key...${NC}"
gcloud iam service-accounts keys create "credentials/gcs-credentials.json" \
    --iam-account="$SERVICE_ACCOUNT_EMAIL"

# Set permissions
chmod 600 credentials/gcs-credentials.json

echo ""
echo -e "${GREEN}✅ Service account setup complete!${NC}"
echo ""
echo "📁 Files created:"
echo "  - credentials/gcs-credentials.json"
echo ""
echo "🔧 Next steps:"
echo "  1. Update your .env.local with your GCS bucket configuration"
echo "  2. Run: docker-compose up --build"
echo ""
echo "🔒 Security:"
echo "  - Keep the credentials file secure"
echo "  - Never commit it to version control"
echo "  - The file is already in .gitignore"
