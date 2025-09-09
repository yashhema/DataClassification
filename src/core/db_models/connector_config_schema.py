# src/core/db_models/connector_config_schema.py
"""
Defines the database schema for storing connector-specific configurations,
such as custom SQL queries for different database versions and environments.
"""

from typing import Dict, Any

from sqlalchemy import (
    String, Integer, JSON, Index, Text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .base import Base

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # Import the Pydantic model for documentation purposes
    from core.config.config_models import ConnectorConfiguration as PydanticConnectorConfig

class ConnectorConfiguration(Base):
    __tablename__ = 'ConnectorConfigurations'
    __table_args__ = (
        Index('uq_connector_config', 'ConnectorType', 'ConfigName', unique=True),
    )
    __doc__ = """
    Stores flexible, connector-specific configurations. This single table can
    hold query sets, settings, and other parameters for any number of
    connectors, avoiding the need for schema changes when adding new ones.
    """
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ConnectorType: Mapped[str] = mapped_column(String(100), nullable=False, index=True, comment="The type of connector this configuration applies to (e.g., 'sqlserver', 'postgres').")
    ConfigName: Mapped[str] = mapped_column(String(255), nullable=False, comment="A name for this specific configuration set (e.g., 'default', 'performance_tuning').")
    
    # UPDATED COMMENT: This now references the Pydantic model for its structure.
    Configuration: Mapped[Dict[str, Any]] = mapped_column(
        JSON, 
        nullable=False,
        comment="JSON object holding the connector configuration. The structure is defined by the 'ConnectorConfiguration' Pydantic model in core/config/config_models.py."
    )

