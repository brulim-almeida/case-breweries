"""
Utils package for Breweries Data Lake
"""

from .delta_spark import initialize_spark, stop_spark

__all__ = ["initialize_spark", "stop_spark"]
