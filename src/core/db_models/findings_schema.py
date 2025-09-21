# src/core/db_models/findings_schema.py
"""
Defines the database schema for storing classification scan findings,
mirroring the final SQL Server schema design.

This schema uses a two-table, decoupled approach for performance:
1.  scan_finding_summaries: The main catalog, storing aggregated summaries.
    Uniqueness is enforced by a hash of the finding's context.
2.  scan_finding_occurrences: An optional, high-volume table for storing
    the details of every individual finding.
"""

from typing import Optional


from sqlalchemy import (
    String, Integer, Float, LargeBinary, Text, Index
)

from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ScanFindingSummary(Base):
    __tablename__ = 'scan_finding_summaries'
    __table_args__ = (
        Index('ix_scan_finding_summaries_scan_job_id', 'scan_job_id'),
        Index('ix_scan_finding_summaries_data_source_id', 'data_source_id'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores an aggregated summary of findings. A SHA-256 hash of the core
    context fields (finding_key_hash) is used to uniquely identify a finding
    for a specific object and classifier, avoiding index length issues.
    """

    # A SHA-256 hash (32 bytes) of the core context fields - now PRIMARY KEY
    finding_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)

    # === Core Context Fields ===
    scan_job_id: Mapped[str] = mapped_column(String(255), nullable=False)
    data_source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    classifier_id: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)

    # === Fields for Structured Data (e.g., Databases) ===
    schema_name: Mapped[Optional[str]] = mapped_column(String(255))
    table_name: Mapped[Optional[str]] = mapped_column(String(255))
    field_name: Mapped[Optional[str]] = mapped_column(String(255))

    # === Fields for Unstructured Data (e.g., Files) ===
    file_path: Mapped[Optional[str]] = mapped_column(String(4000))
    file_name: Mapped[Optional[str]] = mapped_column(String(255))
    file_extension: Mapped[Optional[str]] = mapped_column(String(50))

    # === Aggregated Finding Statistics ===
    finding_count: Mapped[int] = mapped_column(Integer, nullable=False)
    average_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    max_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    # Stores a JSON string of sample findings
    sample_findings: Mapped[Optional[str]] = mapped_column(Text)

    # === Data Quality and Source Statistics ===
    total_rows_in_source: Mapped[Optional[int]] = mapped_column(Integer)
    non_null_rows_scanned: Mapped[Optional[int]] = mapped_column(Integer)


class ScanFindingOccurrence(Base):
    __tablename__ = 'scan_finding_occurrences'
    __table_args__ = (
        Index('ix_scan_finding_occurrences_finding_key_hash', 'finding_key_hash'),
    )
    __doc__ = """
    Stores the detailed information for every single PII finding occurrence.
    This is a high-volume table that is only populated if configured to do so.
    The link to the summary table is managed at the application level.
    """
    # Using finding_key_hash to link to summaries instead of foreign key
    finding_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    
    # Adding a sequence number to make each occurrence unique within a finding
    occurrence_sequence: Mapped[int] = mapped_column(Integer, primary_key=True)

    # === Finding Details ===
    text: Mapped[str] = mapped_column(Text, nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False)
    start_position: Mapped[int] = mapped_column(Integer, nullable=False)
    end_position: Mapped[int] = mapped_column(Integer, nullable=False)
    # Stores a JSON string of extra context
    context_data: Mapped[Optional[str]] = mapped_column(Text)

    # Composite primary key
    __table_args__ = (
        Index('ix_scan_finding_occurrences_finding_key_hash', 'finding_key_hash'),
        {'extend_existing': True}
    )