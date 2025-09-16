# src/core/credentials/provider_interface.py
from abc import ABC, abstractmethod
from typing import Dict, Any

class ICredentialProvider(ABC):
    @abstractmethod
    async def get_secret(self, store_details: Dict[str, Any]) -> str: # Make this async
        pass