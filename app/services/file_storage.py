import aiohttp
import stamina
import asyncio
from gcloud.aio.storage import Storage
from app import settings

# ToDo. Move this to the template for other integrations needing file support
class CloudFileStorage:
    def __init__(self, bucket_name=None, root_prefix=None):
        self.root_prefix = root_prefix or settings.GCP_BUCKET_ROOT_PREFIX
        self.bucket_name = bucket_name or settings.GCP_BUCKET_NAME
        self._storage_client = None  # Lazy initialization

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = Storage()
        return self._storage_client

    def get_file_fullname(self, integration_id, blob_name):
        # Remove integration_id from path - use only root_prefix and blob_name
        return f"{self.root_prefix}/{blob_name}"

    async def upload_file(self, integration_id, local_file_path, destination_blob_name, metadata=None):
        target_path = self.get_file_fullname(integration_id, destination_blob_name)
        custom_metadata = {"metadata": metadata} if metadata else None
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.upload_from_filename(
                    self.bucket_name, target_path, local_file_path, metadata=custom_metadata
                )

    async def download_file(self, integration_id, source_blob_name, destination_file_path):
        source_path = self.get_file_fullname(integration_id, source_blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.download_to_filename(self.bucket_name, source_path, destination_file_path)

    async def delete_file(self, integration_id, blob_name):
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.delete(self.bucket_name, target_path)

    async def list_files(self, integration_id):
        # List files without integration_id in the path
        blobs = await self.storage_client.list_objects(self.bucket_name, params={"prefix": f"{self.root_prefix}"})
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                # Return only the blob names without the root_prefix
                items = blobs.get('items', [])
                results = [blob['name'].replace(f"{self.root_prefix}/", "") for blob in items if blob['name'].startswith(f"{self.root_prefix}/")]
                return results

    async def get_file_metadata(self, integration_id, blob_name):
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                response = await self.storage_client.download_metadata(self.bucket_name, target_path)
                return response.get('metadata', {})

    async def update_file_metadata(self, integration_id, blob_name, metadata):
        target_path = self.get_file_fullname(integration_id, blob_name)
        custom_metadata = {"metadata": metadata}
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.patch_metadata(self.bucket_name, target_path, custom_metadata)

    async def stream_file(self, integration_id, blob_name):
        """
        Stream file contents from GCS as an async generator.
        This is memory-efficient for large files.
        """
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                stream_response = await self.storage_client.download_stream(self.bucket_name, target_path)
                chunk_size = 8192  # 8KB chunks
                while True:
                    chunk = await stream_response.read(chunk_size)
                    if not chunk:  # End of stream
                        break
                    yield chunk

