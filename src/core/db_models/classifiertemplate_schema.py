# src/core/db_models/classifiertemplate_schema.py
"""
Defines the database schema for Classifier Templates using the SQLAlchemy ORM.
"""

from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Float, Boolean, JSON
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .association_tables import classifier_template_link
from .classifier_schema import Classifier


class ClassifierTemplate(Base):
    __tablename__ = 'classifier_templates'
    __doc__ = "Defines a reusable template that groups classifiers and specifies high-level strategies for a scan."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique string identifier for the template.")
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="A human-readable name for the template.")
    description: Mapped[Optional[str]] = mapped_column(String(1024), comment="A detailed description of the template's purpose.")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, comment="Whether this template is active and can be used in jobs.")
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="JSON object containing all detailed, nested configurations like prediction_weights, cross_column_analysis, etc.")
    classifiers: Mapped[List["Classifier"]] = relationship(
        secondary=classifier_template_link,
        back_populates="templates"
    )