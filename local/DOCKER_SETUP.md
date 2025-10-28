# Docker Setup for GCS Action Runner

This guide explains how to set up the Google Cloud Storage action runner using Docker Compose for local development.

## Prerequisites

1. **Docker and Docker Compose** installed on your machine
2. **Google Cloud Project** with a service account
3. **GCS Bucket** for storing telemetry data files

## Quick Setup

### 1. Set Up Google Cloud Credentials

**Option A: Automated Setup (Recommended)**
```bash
# Quick setup with your project ID
./create-service-account-simple.sh your-gcp-project-id

# Or advanced setup with custom options
./create-service-account.sh --project-id your-gcp-project-id
```

**Option B: Manual Setup**
```bash
# Run the setup script
./setup-credentials.sh

# Or manually:
mkdir -p credentials
# Copy your service account JSON key to:
# credentials/gcs-credentials.json
```

**Option C: Validate Existing Setup**
```bash
# Test your existing credentials
./validate-service-account.sh

# Test with specific bucket
./validate-service-account.sh --bucket your-bucket-name
```

### 2. Configure Environment Variables

Create a `.env.local` file in the `local/` directory:

```bash
# Redis Configuration
REDIS_URL=redis://redis:6379/0

# Google Cloud Storage Configuration
GCS_BUCKET_NAME=your-gcs-bucket-name
GCS_BUCKET_PATH=telemetry-data/
GCS_CREDENTIALS_FILE=/code/credentials/gcs-credentials.json

# Action Configuration
ARCHIVE_DAYS=30
DELETE_AFTER_ARCHIVE_DAYS=90

# FastAPI Configuration
DEBUG=true
LOG_LEVEL=INFO
```

### 3. Start the Services

```bash
# Start all services
docker-compose up --build

# Or run in background
docker-compose up -d --build
```

## Service Details

### FastAPI Container
- **Port**: 8080 (API), 5678 (debugger)
- **Volumes**: 
  - `../app:/code/app` (source code)
  - `./credentials:/code/credentials:ro` (GCS credentials)
- **Environment**: 
  - `GOOGLE_APPLICATION_CREDENTIALS=/code/credentials/gcs-credentials.json`

### Redis Container
- **Port**: 6379
- **Purpose**: State management for processed files

### Pub/Sub Emulator
- **Port**: 8085
- **Purpose**: Local message queue for testing

## Testing the GCS Action

### 1. Check Service Health

```bash
# Check if all services are running
docker-compose ps

# Check FastAPI health
curl http://localhost:8080/

# Check Redis
docker-compose exec redis redis-cli ping
```

### 2. Test the Action Runner

```bash
# Access the FastAPI container
docker-compose exec fastapi bash

# Inside the container, test the action
python -c "
from app.actions import action_handlers
print('Available actions:', list(action_handlers.keys()))
"
```

### 3. Monitor Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f fastapi
docker-compose logs -f redis
```

## File Structure

```
local/
├── credentials/
│   ├── gcs-credentials.json    # Your GCS service account key
│   └── README.md               # Credentials setup guide
├── docker-compose.yml         # Docker services configuration
├── create-service-account.sh           # Full-featured service account creator
├── create-service-account-simple.sh    # Simple service account creator
├── validate-service-account.sh         # Service account validator
├── setup-credentials.sh       # Legacy credentials setup script
├── test-setup.sh             # Docker setup tester
└── DOCKER_SETUP.md           # This file
```

## Service Account Scripts

### Quick Setup Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `create-service-account-simple.sh` | Quick service account creation | `./create-service-account-simple.sh PROJECT_ID` |
| `create-service-account.sh` | Advanced service account creation | `./create-service-account.sh --project-id PROJECT_ID` |
| `validate-service-account.sh` | Test existing credentials | `./validate-service-account.sh --bucket BUCKET_NAME` |

### Required Permissions

The scripts automatically assign these roles:
- **Storage Object Viewer**: Read objects from GCS
- **Storage Object Admin**: Move/delete objects in GCS  
- **Storage Bucket Reader**: List buckets and objects

For detailed information, see [SERVICE_ACCOUNT_SETUP.md](SERVICE_ACCOUNT_SETUP.md).

## Troubleshooting

### Common Issues

1. **Credentials Not Found**
   ```bash
   # Check if credentials file exists
   ls -la credentials/
   
   # Verify file permissions
   ls -la credentials/gcs-credentials.json
   ```

2. **Container Can't Access Credentials**
   ```bash
   # Check volume mount
   docker-compose exec fastapi ls -la /code/credentials/
   
   # Verify environment variable
   docker-compose exec fastapi env | grep GOOGLE_APPLICATION_CREDENTIALS
   ```

3. **GCS Authentication Errors**
   ```bash
   # Test credentials inside container
   docker-compose exec fastapi python -c "
   from google.oauth2 import service_account
   import json
   with open('/code/credentials/gcs-credentials.json') as f:
       creds = service_account.Credentials.from_service_account_info(json.load(f))
   print('Credentials loaded successfully')
   "
   ```

### Debug Mode

The FastAPI container runs with debug mode enabled and includes:
- **Debugger port**: 5678 (for VS Code debugging)
- **Hot reload**: Code changes are automatically reflected
- **Detailed logging**: Set `LOG_LEVEL=DEBUG` in `.env.local`

## Security Notes

- Credentials are mounted as read-only (`:ro`)
- Credentials directory is excluded from git (`.gitignore`)
- Service account should have minimal required permissions
- Never commit real credentials to version control

## Next Steps

1. **Upload test files** to your GCS bucket
2. **Configure the action** with your bucket details
3. **Test file processing** with the action runner
4. **Monitor the logs** for processing results

For more details, see the main [GCS_ACTION_README.md](../GCS_ACTION_README.md).
