"""
SQL query builder from SelectionCriteria.
Handles recursive filter traversal, field mapping, and backend-specific SQL generation.
"""

from typing import Dict, List, Any, Literal, Tuple
from core.models.querymodel import (
    SelectionCriteria, QuerySelectionCriteria, ReferenceSelectionCriteria,
    FilterCondition, ComplexFilter, FilterOperator
)
from core.models.field_mappings import (
    get_field_metadata, is_policy_field, FIELD_MAPPINGS, DataSourceType
)
from core.errors import ProcessingError, ErrorType, ErrorSeverity

class QueryBuilder:
    """
    Builds SQL queries from SelectionCriteria with proper field mapping,
    JOIN logic for policy fields, and backend-specific syntax.
    """
    
    MAX_NESTING_DEPTH = 15
    
    def __init__(
        self,
        data_source: DataSourceType,
        backend: Literal["athena", "yugabyte"]
    ):
        self.data_source = data_source
        self.backend = backend
        self._current_depth = 0
    
    def build_query(
        self,
        selection_criteria: SelectionCriteria,
        datasource_ids: List[str],
        additional_conditions: List[str] = None
    ) -> Tuple[str, bool]:
        """
        Build complete SELECT query from selection criteria.
        
        Returns:
            Tuple[str, bool]: (SQL query, needs_policy_join)
        """
        # Handle reference criteria (would need to fetch referenced policy)
        if isinstance(selection_criteria, ReferenceSelectionCriteria):
            raise ProcessingError(
                message="Reference criteria not yet supported",
                error_type=ErrorType.CONFIGURATION_INVALID,
                severity=ErrorSeverity.MEDIUM,
                context={"referenced_policy_id": selection_criteria.policy_template_id}
            )
        
        query_def = selection_criteria.definition
        
        # Build WHERE clause
        where_parts = []
        needs_policy_join = False
        
        # Add datasource filter
        if datasource_ids:
            ds_list = ', '.join([f"'{ds}'" for ds in datasource_ids])
            where_parts.append(f"datasource_id IN ({ds_list})")
        
        # Process filter conditions
        if isinstance(query_def.filters, list):
            # Simple list of conditions (implicit AND)
            for condition in query_def.filters:
                clause, uses_policy = self._build_filter_condition(condition)
                where_parts.append(clause)
                needs_policy_join = needs_policy_join or uses_policy
        else:
            # Complex filter (nested AND/OR/NOT)
            self._current_depth = 0
            clause, uses_policy = self._build_complex_filter(query_def.filters)
            where_parts.append(f"({clause})")
            needs_policy_join = needs_policy_join or uses_policy
        
        # Add additional conditions
        if additional_conditions:
            where_parts.extend(additional_conditions)
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        
        # Build SELECT query
        query = self._build_select_query(
            where_clause=where_clause,
            needs_policy_join=needs_policy_join,
            query_def=query_def
        )
        
        return query, needs_policy_join
    
    def _build_select_query(
        self,
        where_clause: str,
        needs_policy_join: bool,
        query_def
    ) -> str:
        """Build complete SELECT statement with proper JOINs"""
        
        # Determine base table and alias
        if self.data_source == "DISCOVEREDOBJECT":
            if self.backend == "athena":
                base_table = "discovered_objects"
            else:
                base_table = "discovered_objects"
            alias = "do"
        
        elif self.data_source == "OBJECTMETADATA":
            if self.backend == "athena":
                base_table = "object_metadata"
            else:
                base_table = "object_metadata"
            alias = "om"
        
        elif self.data_source == "SCANFINDINGS":
            if self.backend == "athena":
                base_table = "scan_finding_summaries"
            else:
                base_table = "scan_finding_summaries"
            alias = "sfs"
        
        else:
            raise ProcessingError(
                message=f"Unsupported data source: {self.data_source}",
                error_type=ErrorType.CONFIGURATION_INVALID,
                severity=ErrorSeverity.MEDIUM
            )
        
        # Build SELECT clause
        select_fields = [
            f"{alias}.object_key_hash",
            f"{alias}.object_path",
            f"{alias}.datasource_id"
        ]
        
        # Build FROM clause with optional JOIN
        if needs_policy_join:
            # Subquery to get latest action per object
            if self.backend == "athena":
                policy_subquery = f"""
                    (
                        SELECT object_key_hash, status, action_type, 
                               policy_tags, new_object_path, timestamp_utc,
                               ROW_NUMBER() OVER (
                                   PARTITION BY object_key_hash 
                                   ORDER BY timestamp_utc DESC
                               ) as rn
                        FROM policy_action_results
                    ) par
                """
                join_condition = f"{alias}.object_key_hash = par.object_key_hash AND par.rn = 1"
            else:
                # Yugabyte - use DISTINCT ON
                policy_subquery = f"""
                    (
                        SELECT DISTINCT ON (object_key_hash)
                            object_key_hash, status, action_type,
                            policy_tags, new_object_path, timestamp_utc
                        FROM policy_action_results
                        ORDER BY object_key_hash, timestamp_utc DESC
                    ) par
                """
                join_condition = f"{alias}.object_key_hash = par.object_key_hash"
            
            from_clause = f"""
                FROM {base_table} {alias}
                LEFT JOIN {policy_subquery} ON {join_condition}
            """
        else:
            from_clause = f"FROM {base_table} {alias}"
        
        # Build ORDER BY if specified
        order_by = ""
        if query_def.sort:
            sort_parts = []
            for sort_spec in query_def.sort:
                field_meta = get_field_metadata(self.data_source, sort_spec.field)
                col = field_meta.column_name
                direction = "ASC" if sort_spec.order == "asc" else "DESC"
                sort_parts.append(f"{alias}.{col} {direction}")
            order_by = f"ORDER BY {', '.join(sort_parts)}"
        
        # Build LIMIT/OFFSET if specified
        limit_offset = ""
        if query_def.pagination:
            limit_offset = f"LIMIT {query_def.pagination.limit} OFFSET {query_def.pagination.offset}"
        
        # Assemble complete query
        query = f"""
            SELECT {', '.join(select_fields)}
            {from_clause}
            WHERE {where_clause}
            {order_by}
            {limit_offset}
        """.strip()
        
        return query
    
    def _build_complex_filter(self, filter_node: ComplexFilter) -> Tuple[str, bool]:
        """
        Recursively build SQL from nested ComplexFilter.
        
        Returns:
            Tuple[str, bool]: (SQL clause, uses_policy_fields)
        """
        self._current_depth += 1
        
        if self._current_depth > self.MAX_NESTING_DEPTH:
            raise ProcessingError(
                message=f"Filter nesting depth exceeds maximum ({self.MAX_NESTING_DEPTH})",
                error_type=ErrorType.VALIDATION_CONSTRAINT_VIOLATION,
                severity=ErrorSeverity.MEDIUM,
                context={"max_depth": self.MAX_NESTING_DEPTH}
            )
        
        parts = []
        uses_policy = False
        
        # Process AND conditions
        if filter_node.AND:
            and_parts = []
            for condition in filter_node.AND:
                if isinstance(condition, FilterCondition):
                    clause, uses_p = self._build_filter_condition(condition)
                else:
                    clause, uses_p = self._build_complex_filter(condition)
                and_parts.append(f"({clause})")
                uses_policy = uses_policy or uses_p
            
            if and_parts:
                parts.append(" AND ".join(and_parts))
        
        # Process OR conditions
        if filter_node.OR:
            or_parts = []
            for condition in filter_node.OR:
                if isinstance(condition, FilterCondition):
                    clause, uses_p = self._build_filter_condition(condition)
                else:
                    clause, uses_p = self._build_complex_filter(condition)
                or_parts.append(f"({clause})")
                uses_policy = uses_policy or uses_p
            
            if or_parts:
                parts.append(" OR ".join(or_parts))
        
        # Process NOT conditions
        if filter_node.NOT:
            not_parts = []
            for condition in filter_node.NOT:
                if isinstance(condition, FilterCondition):
                    clause, uses_p = self._build_filter_condition(condition)
                else:
                    clause, uses_p = self._build_complex_filter(condition)
                not_parts.append(f"NOT ({clause})")
                uses_policy = uses_policy or uses_p
            
            if not_parts:
                parts.append(" AND ".join(not_parts))
        
        self._current_depth -= 1
        
        combined = " AND ".join(parts) if parts else "1=1"
        return combined, uses_policy
    
    def _build_filter_condition(self, condition: FilterCondition) -> Tuple[str, bool]:
        """
        Build SQL for single filter condition.
        
        Returns:
            Tuple[str, bool]: (SQL clause, uses_policy_field)
        """
        # Check if this is a policy field
        uses_policy_field = is_policy_field(condition.field)
        
        # Get field metadata
        if uses_policy_field:
            field_meta = get_field_metadata("POLICY_ACTION_RESULTS", condition.field)
            table_alias = "par"
        else:
            field_meta = get_field_metadata(self.data_source, condition.field)
            # Use appropriate alias
            if self.data_source == "DISCOVEREDOBJECT":
                table_alias = "do"
            elif self.data_source == "OBJECTMETADATA":
                table_alias = "om"
            elif self.data_source == "SCANFINDINGS":
                table_alias = "sfs"
            else:
                table_alias = "t"
        
        # Select correct column name for backend
        if self.backend == "athena":
            column = field_meta.athena_column
        else:
            column = field_meta.yugabyte_column
        
        full_column = f"{table_alias}.{column}"
        
        # Build condition based on operator
        if condition.operator == FilterOperator.EQUAL:
            value = self._format_value(condition.value, field_meta.data_type)
            clause = f"{full_column} = {value}"
        
        elif condition.operator == FilterOperator.LESS_THAN:
            value = self._format_value(condition.value, field_meta.data_type)
            clause = f"{full_column} < {value}"
        
        elif condition.operator == FilterOperator.GREATER_THAN:
            value = self._format_value(condition.value, field_meta.data_type)
            clause = f"{full_column} > {value}"
        
        elif condition.operator == FilterOperator.CONTAINS:
            if field_meta.data_type != "str":
                raise ProcessingError(
                    message=f"CONTAINS operator only valid for string fields, got {field_meta.data_type}",
                    error_type=ErrorType.VALIDATION_TYPE_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    context={"field": condition.field, "data_type": field_meta.data_type}
                )
            value_str = condition.value.replace("'", "''")  # Escape quotes
            if self.backend == "athena":
                clause = f"CONTAINS(LOWER({full_column}), LOWER('{value_str}'))"
            else:
                clause = f"LOWER({full_column}) LIKE LOWER('%{value_str}%')"
        
        elif condition.operator == FilterOperator.STARTS_WITH:
            if field_meta.data_type != "str":
                raise ProcessingError(
                    message=f"STARTS_WITH operator only valid for string fields",
                    error_type=ErrorType.VALIDATION_TYPE_ERROR,
                    severity=ErrorSeverity.MEDIUM
                )
            value_str = condition.value.replace("'", "''")
            if self.backend == "athena":
                clause = f"STARTS_WITH(LOWER({full_column}), LOWER('{value_str}'))"
            else:
                clause = f"LOWER({full_column}) LIKE LOWER('{value_str}%')"
        
        elif condition.operator == FilterOperator.TEXT_SEARCH:
            if not field_meta.supports_text_search:
                raise ProcessingError(
                    message=f"TEXT_SEARCH not supported for field {condition.field}",
                    error_type=ErrorType.VALIDATION_CONSTRAINT_VIOLATION,
                    severity=ErrorSeverity.MEDIUM
                )
            
            # Parse keywords (pipe-separated)
            keywords = [k.strip() for k in condition.value.split('|')]
            
            if self.backend == "athena":
                # Use regexp_like with alternation
                pattern = '|'.join(keywords)
                clause = f"regexp_like(LOWER({full_column}), LOWER('{pattern}'))"
            else:
                # Use PostgreSQL full-text search
                tsquery = ' | '.join(keywords)
                clause = f"to_tsvector('english', {full_column}) @@ to_tsquery('english', '{tsquery}')"
        
        else:
            raise ProcessingError(
                message=f"Unknown filter operator: {condition.operator}",
                error_type=ErrorType.VALIDATION_CONSTRAINT_VIOLATION,
                severity=ErrorSeverity.MEDIUM
            )
        
        return clause, uses_policy_field
    
    def _format_value(self, value: Any, data_type: str) -> str:
        """Format value for SQL based on data type"""
        if data_type == "str":
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
        elif data_type in ("int", "float"):
            return str(value)
        elif data_type == "datetime":
            # ISO format
            return f"'{value}'"
        elif data_type == "bytes":
            # Hex representation
            if isinstance(value, bytes):
                return f"0x{value.hex()}"
            return f"0x{value}"
        else:
            return f"'{value}'"
