"""
Data Enrichment Module

This module provides data enrichment capabilities including:
- Geocoding for missing coordinates
- Address standardization
- External data integration
"""

from .geocoding import GeocodeEnricher

__all__ = ['GeocodeEnricher']
