from typing import Dict, Any
from .provider_interface import ICredentialProvider
from .file_provider import FileCredentialProvider

class CredentialManager:
    def __init__(self):
        self._providers: Dict[str, ICredentialProvider] = {
            "file": FileCredentialProvider()
        }

    async def get_password_async(self, store_details: Dict[str, Any]) -> str:
        provider_type = store_details.get("type")
        if not provider_type or provider_type not in self._providers:
            raise ValueError(f"Unsupported credential provider type: {provider_type}")
        
        provider = self._providers[provider_type]
        return await provider.get_secret(store_details)