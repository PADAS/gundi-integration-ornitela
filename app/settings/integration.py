from environs import Env

env = Env()
env.read_env()

# Add your integration-specific settings here
INFILE_STORAGE_BUCKET = env.str("INFILE_STORAGE_BUCKET", None)
if INFILE_STORAGE_BUCKET is None:
    raise RuntimeError(
        "INFILE_STORAGE_BUCKET environment variable is not set. "
        "Please set it to the name of your GCP bucket for file storage."
    )