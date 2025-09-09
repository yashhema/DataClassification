# Create new file: src/worker/search_provider.py

import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

# SQLAlchemy imports for the SQL Provider
from sqlalchemy import select, func, text, and_, or_, not_
from sqlalchemy.sql.elements import BinaryExpression

# Pydantic models for the query structure
from core.models.models import QueryDefinition, Pagination, FilterCondition, ComplexFilter, FilterOperator

# Core system components
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import SystemConfig
from core.db_models.discovery_catalog_schema import DiscoveredObject
from core.db_models.findings_schema import ScanFindingSummary

# Elasticsearch library (this would be a new dependency)
# from elasticsearch import AsyncElasticsearch

class AbstractSearchProvider(ABC):
    """
    Abstract interface for a provider that can execute policy selection queries.
    """
    @abstractmethod
    async def get_object_count(self, query: QueryDefinition) -> int:
        """Returns the total number of objects matching the query."""
        pass

    @abstractmethod
    async def get_object_page(self, query: QueryDefinition, pagination: Pagination) -> List[Dict[str, Any]]:
        """Returns a single page of objects matching the query."""
        pass

class SQLSearchProvider(AbstractSearchProvider):
    """
    An implementation that runs selection queries against the primary SQL database.
    """
    def __init__(self, db_interface: DatabaseInterface):
        self.db = db_interface
        self._operator_map = {
            FilterOperator.EQUAL: lambda c, v: c == v,
            FilterOperator.CONTAINS: lambda c, v: c.contains(v),
            FilterOperator.STARTS_WITH: lambda c, v: c.startswith(v),
            FilterOperator.LESS_THAN: lambda c, v: c < v,
            FilterOperator.GREATER_THAN: lambda c, v: c > v,
        }

    def _get_table_from_source(self, source: str):
        if source == "metadata":
            return DiscoveredObject
        elif source == "findings":
            return ScanFindingSummary
        raise ValueError(f"Invalid query source: {source}")

    def _build_where_clause(self, table, filters: Union[List[FilterCondition], ComplexFilter]) -> Optional[BinaryExpression]:
        """Recursively builds a SQLAlchemy WHERE clause from the filter models."""
        if isinstance(filters, list): # Simple list implies AND
            return and_(*[self._build_where_clause(table, f) for f in filters])
        
        if isinstance(filters, FilterCondition):
            column = getattr(table, filters.field)
            op_func = self._operator_map.get(filters.operator)
            if not op_func:
                raise NotImplementedError(f"SQL operator '{filters.operator.value}' is not implemented.")
            return op_func(column, filters.value)
            
        if isinstance(filters, ComplexFilter):
            and_clauses = [self._build_where_clause(table, f) for f in filters.AND]
            or_clauses = [self._build_where_clause(table, f) for f in filters.OR]
            not_clauses = [not_(self._build_where_clause(table, f)) for f in filters.NOT]
            
            # Combine all clauses, filtering out None values
            all_clauses = [c for c in (and_(*and_clauses), or_(*or_clauses), *not_clauses) if c is not None]
            return and_(*all_clauses) if all_clauses else None

    async def get_object_count(self, query: QueryDefinition) -> int:
        table = self._get_table_from_source(query.source)
        stmt = select(func.count()).select_from(table)
        
        where_clause = self._build_where_clause(table, query.filters)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
            
        async with self.db.get_async_session() as session:
            result = await session.execute(stmt)
            return result.scalar_one()

    async def get_object_page(self, query: QueryDefinition, pagination: Pagination) -> List[Dict[str, Any]]:
        table = self._get_table_from_source(query.source)
        stmt = select(table.ObjectID, table.ObjectPath) # Select only the needed fields
        
        where_clause = self._build_where_clause(table, query.filters)
        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Apply sorting for deterministic pagination
        if query.sort:
            for s in query.sort:
                col = getattr(table, s.field)
                stmt = stmt.order_by(col.asc() if s.order == "asc" else col.desc())
        
        # Apply pagination
        stmt = stmt.offset(pagination.offset).limit(pagination.limit)
        
        async with self.db.get_async_session() as session:
            result = await session.execute(stmt)
            # Convert SQLAlchemy rows to the simple dict format the worker expects
            return [{"ObjectID": row.ObjectID, "ObjectPath": row.ObjectPath} for row in result.all()]

class ElasticsearchSearchProvider(AbstractSearchProvider):
    """
    An implementation that runs selection queries against an Elasticsearch cluster.
    """
    def __init__(self, settings: SystemConfig):
        # In a real implementation, you would use the settings to configure
        # the official Elasticsearch async client.
        # self.client = AsyncElasticsearch(
        #     hosts=[{"host": settings.system.elasticsearch_host, "port": settings.system.elasticsearch_port}]
        # )
        self.settings = settings
        pass # Placeholder for client initialization

    async def get_object_count(self, query: QueryDefinition) -> int:
        # This method would translate the QueryDefinition into the Elasticsearch
        # Query DSL and call the client's .count() method.
        print("Placeholder: Executing Elasticsearch count query.")
        await asyncio.sleep(0.1) # Simulate async call
        return 10000 # Return mock data

    async def get_object_page(self, query: QueryDefinition, pagination: Pagination) -> List[Dict[str, Any]]:
        # This method would translate the QueryDefinition into the Elasticsearch
        # Query DSL and call the client's .search() method with 'from' and 'size'.
        print("Placeholder: Executing Elasticsearch search query.")
        await asyncio.sleep(0.1) # Simulate async call
        # Return mock data in the expected format
        return [
            {"ObjectID": f"es_obj_{(i + pagination.offset)}", "ObjectPath": f"/es/path/to/file_{(i + pagination.offset)}"}
            for i in range(pagination.limit)
        ]

# --- Factory Function ---

def create_search_provider(settings: SystemConfig, db_interface: DatabaseInterface) -> AbstractSearchProvider:
    """
    Factory function that reads the system configuration and returns the
    appropriate search provider instance.
    """
    backend = settings.system.search_backend.lower()
    
    if backend == "elasticsearch":
        if not settings.system.elasticsearch_host:
            raise ValueError("Configuration error: 'search_backend' is 'elasticsearch' but 'elasticsearch_host' is not set.")
        return ElasticsearchSearchProvider(settings)
    elif backend == "sql":
        return SQLSearchProvider(db_interface)
    else:
        raise ValueError(f"Unsupported search_backend specified in configuration: '{backend}'")