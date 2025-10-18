# In a new file, e.g., core/db_models/dictionary_schema.py
from typing import List, Optional, Dict, Any
from sqlalchemy import String, Integer, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base
from core.models.models import DictionaryRule, CrossColumnSupport # <-- Import both

# This forward reference is needed to prevent circular import errors
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .classifier_schema import Classifier

class Dictionary(Base):
    __tablename__ = 'dictionaries'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))

    # These fields are mapped as JSON and will be Python dicts in the code
    column_names: Mapped[Optional[DictionaryRule]] = mapped_column(JSON)
    words: Mapped[Optional[DictionaryRule]] = mapped_column(JSON)
    exact_match: Mapped[Optional[DictionaryRule]] = mapped_column(JSON)
    negative: Mapped[Optional[DictionaryRule]] = mapped_column(JSON)
    
    # --- UPDATED FIELD ---
    cross_column_support: Mapped[Optional[CrossColumnSupport]] = mapped_column(JSON) 

    # Defines the one-to-many relationship back to the classifiers that use this dictionary
    classifiers: Mapped[List["Classifier"]] = relationship(back_populates="dictionary")