# src/core/db_models/calendar_schema.py
"""
Defines the database schema for Calendars and their associated rules
using the SQLAlchemy ORM.
"""

import enum
import datetime
from typing import List, Optional

from sqlalchemy import (
    String, Integer, ForeignKey, Time, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

class DayOfWeek(enum.Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

class Calendar(Base):
    __tablename__ = 'calendars'
    __doc__ = "Stores a calendar, which is a named group of scheduling rules."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    rules: Mapped[List["CalendarRule"]] = relationship(back_populates="calendar", cascade="all, delete-orphan")

class CalendarRule(Base):
    __tablename__ = 'calendar_rules'
    __doc__ = "Stores a single scheduling rule for a specific day within a calendar."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    calendar_id: Mapped[int] = mapped_column(ForeignKey('calendars.id'), nullable=False)
    day_of_week: Mapped[DayOfWeek] = mapped_column(SQLAlchemyEnum(DayOfWeek), nullable=False)
    start_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)
    end_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)
    calendar: Mapped["Calendar"] = relationship(back_populates="rules")