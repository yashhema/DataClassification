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
    String, Integer, Float, LargeBinary, Text, Index,Boolean
)

from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ScanFindingSummary(Base):
    __tablename__ = 'scan_finding_summaries'
    __table_args__ = (
        Index('ix_scan_finding_summaries_scan_job_id', 'scan_job_id'),
        Index('ix_scan_finding_summaries_data_source_id', 'data_source_id'),
        Index('ix_scan_finding_summaries_confidence_tier', 'confidence_tier'),
        Index('ix_scan_finding_summaries_entity_type', 'entity_type'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores an aggregated summary of findings with complete statistics.
    """

    # === Primary Key ===
    finding_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    object_path: Mapped[str] = mapped_column(String(5000), nullable=False)
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
    file_path: Mapped[Optional[str]] = mapped_column(Text)  # Changed to Text for unlimited length
    file_name: Mapped[Optional[str]] = mapped_column(String(5000))  # Increased from 255
    file_extension: Mapped[Optional[str]] = mapped_column(String(50))

    # === Aggregated Finding Statistics ===
    finding_count: Mapped[int] = mapped_column(Integer, nullable=False)
    average_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    max_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    confidence_tier: Mapped[Optional[str]] = mapped_column(String(10))  # NEW: HIGH/MEDIUM/LOW
    sample_findings: Mapped[Optional[str]] = mapped_column(Text)

    # === Data Quality and Source Statistics ===
    total_rows_in_source: Mapped[Optional[int]] = mapped_column(Integer)
    non_null_rows_scanned: Mapped[Optional[int]] = mapped_column(Integer)
    
    # === Column Statistics (NULL for files) ===
    null_percentage: Mapped[Optional[float]] = mapped_column(Float)
    min_length: Mapped[Optional[int]] = mapped_column(Integer)
    max_length: Mapped[Optional[int]] = mapped_column(Integer)
    mean_length: Mapped[Optional[float]] = mapped_column(Float)
    distinct_value_count: Mapped[Optional[int]] = mapped_column(Integer)
    distinct_value_percentage: Mapped[Optional[float]] = mapped_column(Float)
    
    # === Match Statistics ===
    total_regex_matches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    regex_match_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    distinct_regex_matches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    distinct_match_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    column_name_matched: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    words_match_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    words_match_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    exact_match_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    exact_match_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    negative_match_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    negative_match_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    scan_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="The timestamp when the current lease expires.")

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