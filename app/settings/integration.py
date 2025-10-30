from environs import Env

env = Env()
env.read_env()

# Add your integration-specific settings here
INFILE_STORAGE_BUCKET = env.str("INFILE_STORAGE_BUCKET", None)