"""
Data Quality Validation Module

This module provides Great Expectations integration for data quality validation
across Bronze, Silver, and Gold layers of the Medallion architecture.
"""

from .ge_validator import BreweriesDataValidator

__all__ = ['BreweriesDataValidator']
