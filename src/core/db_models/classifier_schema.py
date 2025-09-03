# src/core/db_models/classifier_schema.py
"""
Defines the fully normalized database schema for storing Classifier configurations.
"""

import enum
from typing import List, Optional

from sqlalchemy import (
    String, Float, Integer, ForeignKey, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .association_tables import classifier_template_link

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .classifiertemplate_schema import ClassifierTemplate


class Category(Base):
    __tablename__ = 'classifier_categories'
    __table_args__ = {'extend_existing': True}
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(512))
    classifiers: Mapped[List["Classifier"]] = relationship(back_populates="category")

class Classifier(Base):
    __tablename__ = 'classifiers'
    __table_args__ = {'extend_existing': True}
    __doc__ = "The base table for all PII detectors."
    
    id: Mapped[int] = mapped_column(primary_key=True)
    classifier_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    version: Mapped[str] = mapped_column(String(20), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    category_id: Mapped[int] = mapped_column(ForeignKey('classifier_categories.id'), nullable=False)
    category: Mapped["Category"] = relationship(back_populates="classifiers")
    patterns: Mapped[List["ClassifierPattern"]] = relationship(back_populates="classifier", cascade="all, delete-orphan")
    context_rules: Mapped[List["ClassifierContextRule"]] = relationship(back_populates="classifier", cascade="all, delete-orphan")
    validation_rules: Mapped[List["ClassifierValidationRule"]] = relationship(back_populates="classifier", cascade="all, delete-orphan")
    exclude_list: Mapped[List["ClassifierExcludeTerm"]] = relationship(back_populates="classifier", cascade="all, delete-orphan")
    templates: Mapped[List["ClassifierTemplate"]] = relationship(
        secondary=classifier_template_link,
        back_populates="classifiers"
    )

class ClassifierPattern(Base):
    __tablename__ = 'classifier_patterns'
    __table_args__ = {'extend_existing': True}
    id: Mapped[int] = mapped_column(primary_key=True)
    classifier_id: Mapped[int] = mapped_column(ForeignKey('classifiers.id'), nullable=False)
    classifier: Mapped["Classifier"] = relationship(back_populates="patterns")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    regex: Mapped[str] = mapped_column(String(1024), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)

class RuleType(enum.Enum):
    SUPPORT = "support"
    NEGATIVE_SUPPORT = "negative_support"

class ClassifierContextRule(Base):
    __tablename__ = 'classifier_context_rules'
    id: Mapped[int] = mapped_column(primary_key=True)
    classifier_id: Mapped[int] = mapped_column(ForeignKey('classifiers.id'), nullable=False)
    classifier: Mapped["Classifier"] = relationship(back_populates="context_rules")
    rule_type: Mapped[RuleType] = mapped_column(SQLAlchemyEnum(RuleType), nullable=False)
    regex: Mapped[str] = mapped_column(String(1024), nullable=False)
    window_before: Mapped[int] = mapped_column(Integer, nullable=False)
    window_after: Mapped[int] = mapped_column(Integer, nullable=False)

class ClassifierValidationRule(Base):
    __tablename__ = 'classifier_validation_rules'
    id: Mapped[int] = mapped_column(primary_key=True)
    classifier_id: Mapped[int] = mapped_column(ForeignKey('classifiers.id'), nullable=False)
    classifier: Mapped["Classifier"] = relationship(back_populates="validation_rules")
    validation_fn_name: Mapped[str] = mapped_column(String(255), nullable=False)

class ClassifierExcludeTerm(Base):
    __tablename__ = 'classifier_exclude_list'
    id: Mapped[int] = mapped_column(primary_key=True)
    classifier_id: Mapped[int] = mapped_column(ForeignKey('classifiers.id'), nullable=False)
    classifier: Mapped["Classifier"] = relationship(back_populates="exclude_list")
    term_to_exclude: Mapped[str] = mapped_column(String(255), nullable=False)