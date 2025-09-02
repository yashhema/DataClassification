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

    # The type of connector this configuration applies to (e.g., 'sqlserver', 'postgres').
    ConnectorType: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # A name for this specific configuration set (e.g., 'default_queries', 'performance_tuning').
    ConfigName: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # The actual configuration data, stored as a JSON object.
    # This directly holds the content from a file like sql_server_queries_default.yaml.
    Configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

