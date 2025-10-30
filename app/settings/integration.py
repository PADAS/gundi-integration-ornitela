from environs import Env
import logging

logger = logging.getLogger(__name__)

env = Env()
env.read_env()

# Add your integration-specific settings here
INFILE_STORAGE_BUCKET = env.str("INFILE_STORAGE_BUCKET", None)
if INFILE_STORAGE_BUCKET is None:
    logger.warning(
        "INFILE_STORAGE_BUCKET is not set; some features that require GCS may be disabled in this run."
    )