# Google Cloud Service Account Setup

This guide provides two approaches to create a Google Cloud service account with the necessary permissions for the GCS action runner.

## Prerequisites

1. **Google Cloud CLI** installed and authenticated
2. **Project ID** of your Google Cloud project
3. **Required permissions** in your GCP project:
   - Service Account Admin
   - IAM Admin
   - Storage Admin (for bucket operations)

## Quick Setup (Recommended)

Use the simple script for a fast setup:

```bash
# Replace with your actual project ID
./create-service-account-simple.sh your-gcp-project-id
```

This will:
- Create a service account named `gcs-action-runner`
- Assign the required storage roles
- Generate credentials in `./credentials/gcs-credentials.json`
- Set proper file permissions

## Advanced Setup

Use the full-featured script for more control:

```bash
# Basic usage
./create-service-account.sh --project-id your-gcp-project-id

# Custom configuration
./create-service-account.sh \
  --project-id your-gcp-project-id \
  --name my-gcs-runner \
  --display-name "My GCS Runner" \
  --output-dir ./my-credentials \
  --key-file my-credentials.json
```

### Available Options

| Option | Description | Default |
|--------|-------------|---------|
| `-p, --project-id` | Google Cloud project ID | Required |
| `-n, --name` | Service account name | `gcs-action-runner` |
| `-d, --display-name` | Display name | `GCS Action Runner` |
| `-o, --output-dir` | Output directory | `./credentials` |
| `-k, --key-file` | Key file name | `gcs-credentials.json` |

## Required Roles

The scripts automatically assign these roles to the service account:

| Role | Purpose |
|------|---------|
| `roles/storage.objectViewer` | Read objects from GCS buckets |
| `roles/storage.objectAdmin` | Move/delete objects in GCS |
| `roles/storage.bucketReader` | List buckets and objects |

## Manual Setup (Alternative)

If you prefer to set up manually through the Google Cloud Console:

### 1. Create Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **IAM & Admin** > **Service Accounts**
3. Click **Create Service Account**
4. Fill in the details:
   - **Name**: `gcs-action-runner`
   - **Description**: `Service account for GCS action runner to process telemetry data`
5. Click **Create and Continue**

### 2. Assign Roles

In the **Grant this service account access to project** section, add these roles:
- **Storage Object Viewer**
- **Storage Object Admin** 
- **Storage Bucket Reader**

### 3. Create Key

1. Click on your service account
2. Go to the **Keys** tab
3. Click **Add Key** > **Create new key**
4. Choose **JSON** format
5. Download the key file
6. Save it as `credentials/gcs-credentials.json`

### 4. Set Permissions

```bash
chmod 600 credentials/gcs-credentials.json
```

## Verification

Test that your service account works:

```bash
# Test authentication
export GOOGLE_APPLICATION_CREDENTIALS="./credentials/gcs-credentials.json"
gcloud auth activate-service-account --key-file="./credentials/gcs-credentials.json"

# Test GCS access (replace with your bucket name)
gsutil ls gs://your-bucket-name

# Clean up
gcloud auth revoke
```

## Security Best Practices

### 1. Principle of Least Privilege
- Only assign the minimum required roles
- Regularly review and audit permissions
- Remove unused service accounts

### 2. Key Management
- Rotate keys regularly (every 90 days recommended)
- Store keys securely (not in version control)
- Use environment variables for key paths in production

### 3. Access Control
- Limit who can create/modify service accounts
- Use IAM conditions for additional security
- Monitor service account usage

## Troubleshooting

### Common Issues

1. **Permission Denied**
   ```bash
   # Check if you have the required permissions
   gcloud projects get-iam-policy your-project-id
   ```

2. **Service Account Already Exists**
   ```bash
   # List existing service accounts
   gcloud iam service-accounts list
   
   # Delete if needed (be careful!)
   gcloud iam service-accounts delete SERVICE_ACCOUNT_EMAIL
   ```

3. **Authentication Issues**
   ```bash
   # Re-authenticate
   gcloud auth login
   
   # Check current authentication
   gcloud auth list
   ```

### Validation Commands

```bash
# Check service account exists
gcloud iam service-accounts describe SERVICE_ACCOUNT_EMAIL

# Check assigned roles
gcloud projects get-iam-policy PROJECT_ID \
  --flatten="bindings[].members" \
  --format="table(bindings.role)" \
  --filter="bindings.members:SERVICE_ACCOUNT_EMAIL"

# Test GCS access
gsutil ls gs://BUCKET_NAME
```

## Next Steps

After creating the service account:

1. **Update your `.env.local`** with your GCS bucket configuration
2. **Start the services**: `docker-compose up --build`
3. **Test the action runner** with your telemetry files
4. **Monitor the logs** for any issues

## File Structure

After running the setup scripts:

```
local/
├── credentials/
│   └── gcs-credentials.json    # Your service account key
├── create-service-account.sh           # Full-featured script
├── create-service-account-simple.sh    # Simple script
└── SERVICE_ACCOUNT_SETUP.md            # This guide
```

## Support

If you encounter issues:

1. Check the [Google Cloud IAM documentation](https://cloud.google.com/iam/docs)
2. Verify your project permissions
3. Ensure the service account has the required roles
4. Test GCS access manually with `gsutil`

For more information about the GCS action runner, see [DOCKER_SETUP.md](DOCKER_SETUP.md).
