"""TPC-DS DataFrame API query implementations.

This package contains 34 TPC-DS queries implemented using pure PySpark
DataFrame API (no SQL-specific features like CTEs, ROLLUP/CUBE, EXISTS).
"""
from .tpcds_dataframe_queries import (
    COMPATIBLE_QUERIES,
    get_query_implementation,
    list_compatible_queries,
    list_incompatible_queries,
    run_query,
)


__all__ = [
    'COMPATIBLE_QUERIES',
    'get_query_implementation',
    'run_query',
    'list_compatible_queries',
    'list_incompatible_queries',
]
