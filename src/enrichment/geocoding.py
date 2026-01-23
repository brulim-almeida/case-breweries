"""
Geocoding Enrichment Module

This module enriches brewery data by adding geographic coordinates
for entries that are missing them.

Uses Nominatim (OpenStreetMap) API - free and no API key required.
Respects rate limits (1 request/second) as per Nominatim usage policy.

Author: Data Engineering Team
"""

import logging
import time
from typing import Optional, Dict, Any, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GeocodeEnricher:
    """
    Enrich brewery data with geographic coordinates using geocoding services.
    
    This class provides methods to:
    1. Identify breweries without coordinates
    2. Geocode addresses using Nominatim API
    3. Update records with new coordinates
    4. Track success/failure rates
    
    Example:
        >>> enricher = GeocodeEnricher(spark)
        >>> enriched_df = enricher.enrich_coordinates(df)
        >>> print(f"Enriched {enricher.stats['geocoded_count']} breweries")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        rate_limit_delay: float = 1.0,  # Reduzido para 1.0s
        timeout: int = 5,  # Reduzido timeout
        user_agent: str = "BreweriesDataLake/1.0"
    ):
        """
        Initialize the Geocode Enricher.
        
        Args:
            spark: SparkSession instance
            rate_limit_delay: Delay between requests in seconds (Nominatim requires 1/sec)
            timeout: Request timeout in seconds
            user_agent: User agent for API requests
        """
        self.spark = spark
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.user_agent = user_agent
        self.base_url = "https://nominatim.openstreetmap.org/search"
        
        # Statistics tracking
        self.stats = {
            'total_missing': 0,
            'geocoded_count': 0,
            'failed_count': 0,
            'skipped_count': 0
        }
        
        # Setup session with retries
        self.session = self._create_session()
        
        logger.info("GeocodeEnricher initialized")
        logger.info(f"Rate limit: 1 request per {rate_limit_delay} seconds")
    
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with retry logic.
        
        Returns:
            Configured requests Session
        """
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def geocode_address(
        self,
        street: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        postal_code: Optional[str] = None
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Geocode an address using Nominatim API.
        
        Strategy:
        1. Try full address first
        2. If fails, try city + state + country
        3. If fails, try city + country
        
        Args:
            street: Street address
            city: City name
            state: State/province
            country: Country name
            postal_code: Postal code
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails
        """
        # Build address string with available components
        address_parts = []
        
        if street:
            address_parts.append(street)
        if city:
            address_parts.append(city)
        if state:
            address_parts.append(state)
        if country:
            address_parts.append(country)
        if postal_code:
            address_parts.append(postal_code)
        
        if not address_parts:
            logger.warning("No address components provided for geocoding")
            return None, None
        
        address = ", ".join(address_parts)
        
        # Make API request
        params = {
            'q': address,
            'format': 'json',
            'limit': 1,
            'addressdetails': 1
        }
        
        headers = {
            'User-Agent': self.user_agent
        }
        
        try:
            logger.debug(f"Geocoding: {address}")
            
            response = self.session.get(
                self.base_url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            results = response.json()
            
            if results and len(results) > 0:
                lat = float(results[0]['lat'])
                lon = float(results[0]['lon'])
                logger.debug(f"✅ Geocoded: {address} → ({lat}, {lon})")
                # Respect rate limit
                time.sleep(self.rate_limit_delay)
                return lat, lon
            else:
                # Try fallback with just city + country
                if city and country and len(address_parts) > 2:
                    logger.debug(f"Trying fallback: {city}, {country}")
                    # Don't sleep here - will sleep after fallback returns
                    return self.geocode_address(city=city, country=country)
                
                logger.warning(f"❌ No results for: {address}")
                # Respect rate limit even on failure
                time.sleep(self.rate_limit_delay)
                return None, None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API error for {address}: {e}")
            # Respect rate limit even on error
            time.sleep(self.rate_limit_delay)
            return None, None
        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Parsing error for {address}: {e}")
            # Respect rate limit even on error
            time.sleep(self.rate_limit_delay)
            return None, None
    
    def enrich_coordinates(
        self,
        df: DataFrame,
        max_records: Optional[int] = None,
        batch_size: int = 100
    ) -> DataFrame:
        """
        Enrich DataFrame with missing coordinates.
        
        Process:
        1. Identify records without coordinates
        2. Collect addresses to Pandas (for API calls)
        3. Geocode each address
        4. Join results back to original DataFrame
        
        Args:
            df: Input DataFrame
            max_records: Maximum number of records to geocode (for testing)
            batch_size: Process in batches and save progress
            
        Returns:
            DataFrame with enriched coordinates
        """
        logger.info("=" * 80)
        logger.info("STARTING COORDINATE ENRICHMENT")
        logger.info("=" * 80)
        
        # Filter records without coordinates
        missing_coords = df.filter(
            F.col("latitude").isNull() | F.col("longitude").isNull()
        )
        
        self.stats['total_missing'] = missing_coords.count()
        logger.info(f"Found {self.stats['total_missing']:,} breweries without coordinates")
        
        if self.stats['total_missing'] == 0:
            logger.info("No coordinates to enrich. Returning original DataFrame.")
            return df
        
        # Limit if specified (for testing)
        if max_records:
            logger.info(f"Limiting to {max_records} records for geocoding")
            missing_coords = missing_coords.limit(max_records)
        
        # Collect to Pandas for API calls
        logger.info("Collecting addresses for geocoding...")
        to_geocode_pd = missing_coords.select(
            "id",
            "name",
            "street",
            "city",
            "state",
            "country_normalized",
            "postal_code"
        ).toPandas()
        
        logger.info(f"Processing {len(to_geocode_pd)} addresses...")
        estimated_time = len(to_geocode_pd) * self.rate_limit_delay / 60
        logger.info(f"⏱️  Estimated minimum time: ~{estimated_time:.1f} minutes "
                   f"(at {self.rate_limit_delay}s per request)")
        
        # Geocode each address
        results = []
        start_time = time.time()
        
        for idx, row in to_geocode_pd.iterrows():
            if idx > 0 and idx % 10 == 0:
                elapsed = time.time() - start_time
                rate = idx / elapsed
                remaining = (len(to_geocode_pd) - idx) / rate / 60
                logger.info(f"Progress: {idx}/{len(to_geocode_pd)} "
                           f"({idx/len(to_geocode_pd)*100:.1f}%) - "
                           f"Geocoded: {self.stats['geocoded_count']}, "
                           f"Failed: {self.stats['failed_count']} - "
                           f"ETA: ~{remaining:.1f} min")
            
            lat, lon = self.geocode_address(
                street=row['street'],
                city=row['city'],
                state=row['state'],
                country=row['country_normalized'],
                postal_code=row['postal_code']
            )
            
            if lat and lon:
                results.append({
                    'id': row['id'],
                    'geocoded_latitude': lat,
                    'geocoded_longitude': lon,
                    'geocoded': True
                })
                self.stats['geocoded_count'] += 1
            else:
                self.stats['failed_count'] += 1
        
        logger.info(f"Geocoding complete: {self.stats['geocoded_count']} successful, "
                   f"{self.stats['failed_count']} failed")
        
        if not results:
            logger.warning("No coordinates were geocoded successfully")
            return df
        
        # Convert results to Spark DataFrame
        results_df = self.spark.createDataFrame(results)
        
        # Join with original DataFrame and update coordinates
        enriched_df = (df
                      .join(results_df, on='id', how='left')
                      .withColumn(
                          'latitude',
                          F.when(F.col('geocoded') == True, F.col('geocoded_latitude'))
                           .otherwise(F.col('latitude'))
                      )
                      .withColumn(
                          'longitude',
                          F.when(F.col('geocoded') == True, F.col('geocoded_longitude'))
                           .otherwise(F.col('longitude'))
                      )
                      .drop('geocoded_latitude', 'geocoded_longitude', 'geocoded'))
        
        # Update has_coordinates flag
        enriched_df = enriched_df.withColumn(
            'has_coordinates',
            F.when(
                (F.col('latitude').isNotNull()) & (F.col('longitude').isNotNull()),
                True
            ).otherwise(False)
        )
        
        logger.info("=" * 80)
        logger.info("ENRICHMENT COMPLETE")
        logger.info(f"Total missing: {self.stats['total_missing']:,}")
        logger.info(f"Successfully geocoded: {self.stats['geocoded_count']:,}")
        logger.info(f"Failed: {self.stats['failed_count']:,}")
        logger.info(f"Success rate: {self.stats['geocoded_count']/self.stats['total_missing']*100:.2f}%")
        logger.info("=" * 80)
        
        return enriched_df
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get enrichment statistics.
        
        Returns:
            Dictionary with statistics
        """
        return self.stats.copy()
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
        logger.info("GeocodeEnricher session closed")
