#!/bin/bash

# Helper script to create a Google Cloud service account for GCS action runner
# This script automates the creation of a service account with storage permissions

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
PROJECT_ID=""
SERVICE_ACCOUNT_NAME="gcs-action-runner"
SERVICE_ACCOUNT_DISPLAY_NAME="GCS Action Runner"
SERVICE_ACCOUNT_DESCRIPTION="Service account for GCS action runner to process telemetry data"
OUTPUT_DIR="./credentials"
KEY_FILE="gcs-credentials.json"

# Required roles for GCS action runner
REQUIRED_ROLES=(
    "roles/storage.objectUser"
)

#"roles/storage.objectViewer"      # Read objects from GCS
#"roles/storage.objectAdmin"       # Move/delete objects in GCS
#"roles/storage.bucketReader"      # List buckets and objects
# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Create a Google Cloud service account for GCS action runner"
    echo ""
    echo "Options:"
    echo "  -p, --project-id PROJECT_ID     Google Cloud project ID (required)"
    echo "  -n, --name NAME                Service account name (default: gcs-action-runner)"
    echo "  -d, --display-name NAME        Display name (default: GCS Action Runner)"
    echo "  -o, --output-dir DIR           Output directory (default: ./credentials)"
    echo "  -k, --key-file FILE            Key file name (default: gcs-credentials.json)"
    echo "  -h, --help                     Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --project-id my-project-123"
    echo "  $0 -p my-project-123 -n my-gcs-runner -o ./my-credentials"
    echo ""
    echo "Required permissions:"
    echo "  - Service Account Admin"
    echo "  - IAM Admin"
    echo "  - Storage Admin (for bucket operations)"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        -n|--name)
            SERVICE_ACCOUNT_NAME="$2"
            shift 2
            ;;
        -d|--display-name)
            SERVICE_ACCOUNT_DISPLAY_NAME="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -k|--key-file)
            KEY_FILE="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$PROJECT_ID" ]; then
    print_error "Project ID is required. Use -p or --project-id"
    show_usage
    exit 1
fi

# Validate gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it first:"
    echo "  https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    print_error "Not authenticated with gcloud. Please run:"
    echo "  gcloud auth login"
    exit 1
fi

# Check if project exists and user has access
print_info "Validating project access..."
if ! gcloud projects describe "$PROJECT_ID" &> /dev/null; then
    print_error "Project '$PROJECT_ID' not found or you don't have access to it"
    exit 1
fi

# Set the active project
gcloud config set project "$PROJECT_ID"
print_success "Using project: $PROJECT_ID"

# Create output directory
mkdir -p "$OUTPUT_DIR"
print_info "Output directory: $OUTPUT_DIR"

# Check if service account already exists
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" &> /dev/null; then
    print_warning "Service account '$SERVICE_ACCOUNT_NAME' already exists"
    read -p "Do you want to continue and create a new key? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Exiting without changes"
        exit 0
    fi
else
    # Create service account
    print_info "Creating service account: $SERVICE_ACCOUNT_NAME"
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="$SERVICE_ACCOUNT_DISPLAY_NAME" \
        --description="$SERVICE_ACCOUNT_DESCRIPTION" \
        --quiet
    
    print_success "Service account created: $SERVICE_ACCOUNT_EMAIL"
fi

# Assign required roles
print_info "Assigning required roles..."
for role in "${REQUIRED_ROLES[@]}"; do
    print_info "  Assigning role: $role"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="$role" \
        --quiet \
        --condition=None
done

print_success "All required roles assigned"

# Create and download service account key
print_info "Creating service account key..."
KEY_PATH="$OUTPUT_DIR/$KEY_FILE"
gcloud iam service-accounts keys create "$KEY_PATH" \
    --iam-account="$SERVICE_ACCOUNT_EMAIL" \
    --quiet

print_success "Service account key created: $KEY_PATH"

# Set secure permissions on the key file
chmod 600 "$KEY_PATH"
print_success "Set secure permissions on key file"

# Validate the key file
print_info "Validating key file..."
if python3 -m json.tool "$KEY_PATH" > /dev/null 2>&1; then
    print_success "Key file is valid JSON"
else
    print_error "Key file is not valid JSON"
    exit 1
fi

# Test authentication with the new key
print_info "Testing authentication..."
export GOOGLE_APPLICATION_CREDENTIALS="$KEY_PATH"
if gcloud auth activate-service-account --key-file="$KEY_PATH" &> /dev/null; then
    print_success "Authentication test passed"
    gcloud auth revoke &> /dev/null || true  # Clean up
else
    print_warning "Authentication test failed, but key file was created"
fi

# Show summary
echo ""
print_success "🎉 Service account setup complete!"
echo ""
echo "📋 Summary:"
echo "  Project ID: $PROJECT_ID"
echo "  Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "  Key File: $KEY_PATH"
echo "  Roles Assigned:"
for role in "${REQUIRED_ROLES[@]}"; do
    echo "    - $role"
done
echo ""
echo "🔧 Next steps:"
echo "  1. Update your .env.local with your GCS bucket configuration"
echo "  2. Run: docker-compose up --build"
echo "  3. Test the action runner"
echo ""
echo "🔒 Security notes:"
echo "  - Keep the key file secure and never commit it to version control"
echo "  - The key file has been added to .gitignore"
echo "  - Consider rotating keys regularly for security"
echo ""
echo "📚 For more information, see: DOCKER_SETUP.md"
