import json
import aiofiles
from typing import Dict, Any
from .provider_interface import ICredentialProvider

class FileCredentialProvider(ICredentialProvider):
    def __init__(self, file_path: str = "config/credentials.json"):
        self.file_path = file_path
        self._secrets = None

    async def _load_secrets(self):
        if self._secrets is None:
            try:
                async with aiofiles.open(self.file_path, 'r') as f:
                    self._secrets = json.loads(await f.read())
            except (FileNotFoundError, json.JSONDecodeError) as e:
                raise IOError(f"Could not load credentials file at {self.file_path}: {e}") from e

    async def get_secret(self, store_details: Dict[str, Any]) -> str:
        await self._load_secrets()
        key = store_details.get("key")
        if not key:
            raise ValueError("File provider requires a 'key' in store_details.")
        
        password = self._secrets.get(key)
        if password is None:
            raise ValueError(f"Secret key '{key}' not found in credentials file.")
            
        return password