# src/core/db_models/base.py
"""
Defines the single, shared DeclarativeBase for all SQLAlchemy ORM models
in the application.
"""

from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    """The base class for all ORM models."""
