# src/core/db_models/association_tables.py
"""
Defines SQLAlchemy association tables for many-to-many relationships.

Placing these tables in a separate file resolves circular import issues
between the models they link.
"""

from sqlalchemy import (
    Table, Column, String, Integer, Float, Boolean, ForeignKey
)

# Import the shared base for metadata
from .base import Base

# Links ClassifierTemplate and Classifier models
ClassifierTemplateLink = Table(
    "classifier_template_link",
    Base.metadata,
    Column("template_id", String(255), ForeignKey("classifier_templates.template_id"), primary_key=True),
    Column("classifier_id", String(255), ForeignKey("classifiers.classifier_id"), primary_key=True),
    Column("weight", Float, nullable=False, default=1.0),
    Column("is_required", Boolean, nullable=False, default=False),
    extend_existing=True  
)

# Links DataSource and Tag models
DataSourceTagLink = Table(
    "datasource_tag_link",
    Base.metadata,
    Column("datasource_id", String(255), ForeignKey("datasources.datasource_id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
    extend_existing=True 
)

# Links DataSource and OverrideGroup models
DatasourceToOverrideGroupLink = Table(
    "datasource_to_override_group_link",
    Base.metadata,
    Column("datasource_id", String(255), ForeignKey("datasources.datasource_id"), primary_key=True),
    Column("override_group_id", Integer, ForeignKey("override_groups.id"), primary_key=True),
    extend_existing=True
)