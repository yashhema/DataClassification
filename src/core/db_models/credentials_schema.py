# src/core/db_models/credentials_schema.py
"""
Defines the database schema for the Credentials table using the SQLAlchemy ORM.

This table is designed to store metadata and secure references to credentials,
not the sensitive secrets themselves.
"""

from typing import Optional, Dict, Any

from sqlalchemy import String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class Credential(Base):
    __tablename__ = 'credentials'
    __doc__ = """
    Stores references to credentials. IMPORTANT: Passwords are NOT stored here;
    this table references secrets stored in a secure external system like
    HashiCorp Vault or AWS Secrets Manager.
    
    Usage:
    The ConfigurationManager loads these records. When a component needs a
    password, it provides a credential_id, and the CredentialManager uses
    the 'store_details' to fetch the secret from the secure vault at runtime.
    """
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Example: "cred_001"
    # The unique string identifier from the credentials.yaml file.
    credential_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique string identifier for the credential reference.")
    
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="A human-readable name for the credential.")
    description: Mapped[Optional[str]] = mapped_column(String(1024), comment="A brief description of the credential's purpose.")
    username: Mapped[Optional[str]] = mapped_column(String(255), comment="The username associated with the credential.")
    domain: Mapped[Optional[str]] = mapped_column(String(255), comment="The domain, if applicable (e.g., for Windows authentication).")

    # Example: {"type": "VAULT", "path": "secret/data/classification/smb_scanner"}
    # A JSON object containing the details needed to retrieve the secret from
    # a secure store. It should NOT contain the secret itself.
    store_details: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="JSON object with details for retrieving the secret from a secure external store.")