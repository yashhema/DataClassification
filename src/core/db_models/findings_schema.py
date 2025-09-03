# src/core/db_models/findings_schema.py
"""
Defines the database schema for storing classification scan findings,
mirroring the final SQL Server schema design.

This schema uses a two-table, decoupled approach for performance:
1.  ScanFindingSummaries: The main catalog, storing aggregated summaries.
    Uniqueness is enforced by a hash of the finding's context.
2.  ScanFindingOccurrences: An optional, high-volume table for storing
    the details of every individual finding.
"""

from typing import Optional, Dict, Any

from sqlalchemy import (
    String, Integer, Float, LargeBinary, Text, Index
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .base import Base

class ScanFindingSummary(Base):
    __tablename__ = 'ScanFindingSummaries'
    __table_args__ = (
        Index('uq_finding_key_hash', 'FindingKeyHash', unique=True),
        Index('IX_ScanFindingSummaries_ScanJobID', 'ScanJobID'),
        Index('IX_ScanFindingSummaries_DataSourceID', 'DataSourceID'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores an aggregated summary of findings. A SHA-256 hash of the core
    context fields (FindingKeyHash) is used to uniquely identify a finding
    for a specific object and classifier, avoiding index length issues.
    """
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # A SHA-256 hash (32 bytes) of the core context fields.
    FindingKeyHash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)

    # === Core Context Fields ===
    ScanJobID: Mapped[str] = mapped_column(String(255), nullable=False)
    DataSourceID: Mapped[str] = mapped_column(String(255), nullable=False)
    ClassifierID: Mapped[str] = mapped_column(String(255), nullable=False)
    EntityType: Mapped[str] = mapped_column(String(100), nullable=False)

    # === Fields for Structured Data (e.g., Databases) ===
    SchemaName: Mapped[Optional[str]] = mapped_column(String(255))
    TableName: Mapped[Optional[str]] = mapped_column(String(255))
    FieldName: Mapped[Optional[str]] = mapped_column(String(255))

    # === Fields for Unstructured Data (e.g., Files) ===
    FilePath: Mapped[Optional[str]] = mapped_column(String(4000))
    FileName: Mapped[Optional[str]] = mapped_column(String(255))
    FileExtension: Mapped[Optional[str]] = mapped_column(String(50))

    # === Aggregated Finding Statistics ===
    FindingCount: Mapped[int] = mapped_column(Integer, nullable=False)
    AverageConfidence: Mapped[float] = mapped_column(Float, nullable=False)
    MaxConfidence: Mapped[float] = mapped_column(Float, nullable=False)
    # Stores a JSON string of sample findings
    SampleFindings: Mapped[Optional[str]] = mapped_column(Text)

    # === Data Quality and Source Statistics ===
    TotalRowsInSource: Mapped[Optional[int]] = mapped_column(Integer)
    NonNullRowsScanned: Mapped[Optional[int]] = mapped_column(Integer)


class ScanFindingOccurrence(Base):
    __tablename__ = 'ScanFindingOccurrences'
    __table_args__ = (
        Index('IX_ScanFindingOccurrences_SummaryID', 'SummaryID'),
    )
    __doc__ = """
    Stores the detailed information for every single PII finding occurrence.
    This is a high-volume table that is only populated if configured to do so.
    The link to the summary table is managed at the application level.
    """
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # This ID links back to the parent record in the ScanFindingSummaries table.
    SummaryID: Mapped[int] = mapped_column(Integer, nullable=False)

    # === Finding Details ===
    Text: Mapped[str] = mapped_column(Text, nullable=False)
    ConfidenceScore: Mapped[float] = mapped_column(Float, nullable=False)
    StartPosition: Mapped[int] = mapped_column(Integer, nullable=False)
    EndPosition: Mapped[int] = mapped_column(Integer, nullable=False)
    # Stores a JSON string of extra context
    ContextData: Mapped[Optional[str]] = mapped_column(Text)

