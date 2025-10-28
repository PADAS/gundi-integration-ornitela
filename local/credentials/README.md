# Google Cloud Storage Credentials

This directory contains the Google Cloud Service Account credentials needed for the GCS action runner.

## Setup Instructions

1. **Create a Google Cloud Service Account:**
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Navigate to IAM & Admin > Service Accounts
   - Create a new service account or use an existing one
   - Grant the following permissions:
     - Storage Object Viewer (to read files)
     - Storage Object Admin (to move/delete files)

2. **Download the Service Account Key:**
   - Click on your service account
   - Go to the "Keys" tab
   - Click "Add Key" > "Create new key"
   - Choose JSON format
   - Download the key file

3. **Replace the Sample Credentials:**
   - Replace the contents of `gcs-credentials.json` with your actual service account key
   - Update the following fields in the JSON:
     - `project_id`: Your GCP project ID
     - `private_key_id`: From your downloaded key
     - `private_key`: From your downloaded key (keep the newlines)
     - `client_email`: From your downloaded key
     - `client_id`: From your downloaded key
     - `client_x509_cert_url`: From your downloaded key

4. **Security Note:**
   - Never commit real credentials to version control
   - Add `local/credentials/` to your `.gitignore` file
   - The credentials file is mounted as read-only in the container

## File Structure

```
local/
├── credentials/
│   ├── gcs-credentials.json  # Your actual service account key
│   └── README.md           # This file
└── docker-compose.yml     # Updated with credentials mount
```

## Environment Variables

The docker-compose setup automatically sets:
- `GOOGLE_APPLICATION_CREDENTIALS=/code/credentials/gcs-credentials.json`

This allows the `CloudFileStorage` service to authenticate with Google Cloud Storage.
