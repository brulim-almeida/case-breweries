"""
Open Brewery DB API Client

This module provides a robust client for interacting with the Open Brewery DB API,
including pagination, retry logic, error handling, and rate limiting.
"""

import time
import requests
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

from src.config.settings import Settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BreweryAPIClient:
    """
    Client for interacting with the Open Brewery DB API.
    
    Features:
    - Automatic pagination handling
    - Retry logic with exponential backoff
    - Request timeout management
    - Error handling and logging
    - Rate limiting compliance
    
    Example:
        >>> client = BreweryAPIClient()
        >>> breweries = client.get_all_breweries()
        >>> print(f"Fetched {len(breweries)} breweries")
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
        retry_delay: Optional[int] = None
    ):
        """
        Initialize the Brewery API Client.
        
        Args:
            base_url (str, optional): Base URL for the API. Defaults to Settings.BREWERY_API_BASE_URL
            timeout (int, optional): Request timeout in seconds. Defaults to Settings.BREWERY_API_TIMEOUT
            retries (int, optional): Number of retry attempts. Defaults to Settings.BREWERY_API_RETRIES
            retry_delay (int, optional): Delay between retries. Defaults to Settings.BREWERY_API_RETRY_DELAY
        """
        self.base_url = base_url or Settings.BREWERY_API_BASE_URL
        self.timeout = timeout or Settings.BREWERY_API_TIMEOUT
        self.retries = retries or Settings.BREWERY_API_RETRIES
        self.retry_delay = retry_delay or Settings.BREWERY_API_RETRY_DELAY
        self.per_page = Settings.BREWERY_API_PER_PAGE
        
        # Configure session with retry strategy
        self.session = self._create_session()
        
        logger.info(f"Initialized BreweryAPIClient with base_url: {self.base_url}")
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry strategy.
        
        Returns:
            requests.Session: Configured session with retry logic
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make an HTTP GET request to the API with error handling.
        
        Args:
            endpoint (str): API endpoint (e.g., '/breweries')
            params (dict, optional): Query parameters
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If request fails after all retries
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"Making request to: {url} with params: {params}")
            
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout after {self.timeout}s: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}")
            raise
    
    def get_breweries_page(
        self,
        page: int = 1,
        per_page: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of breweries from the API.
        
        Args:
            page (int): Page number (1-indexed)
            per_page (int, optional): Number of results per page
            
        Returns:
            list: List of brewery dictionaries
        """
        params = {
            "page": page,
            "per_page": per_page or self.per_page
        }
        
        logger.info(f"Fetching breweries page {page} (per_page={params['per_page']})")
        
        breweries = self._make_request("/breweries", params=params)
        
        logger.info(f"Successfully fetched {len(breweries)} breweries from page {page}")
        
        return breweries
    
    def get_all_breweries(
        self,
        max_pages: Optional[int] = None,
        delay_between_pages: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Fetch all breweries from the API with automatic pagination.
        
        Args:
            max_pages (int, optional): Maximum number of pages to fetch (for testing)
            delay_between_pages (float): Delay in seconds between page requests
            
        Returns:
            list: List of all brewery dictionaries
        """
        all_breweries = []
        page = 1
        
        logger.info("Starting to fetch all breweries from API")
        
        while True:
            # Check if we've reached max_pages limit
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            try:
                breweries = self.get_breweries_page(page=page)
                
                # If no breweries returned, we've reached the end
                if not breweries:
                    logger.info(f"No more breweries found. Total pages fetched: {page - 1}")
                    break
                
                all_breweries.extend(breweries)
                
                logger.info(
                    f"Progress: Page {page} | "
                    f"Current page: {len(breweries)} breweries | "
                    f"Total: {len(all_breweries)} breweries"
                )
                
                page += 1
                
                # Rate limiting: delay between requests
                if delay_between_pages > 0:
                    time.sleep(delay_between_pages)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                
                # Wait before retrying
                time.sleep(self.retry_delay)
                
                # Retry the same page
                continue
        
        logger.info(f"Successfully fetched all {len(all_breweries)} breweries")
        
        return all_breweries
    
    def get_brewery_by_id(self, brewery_id: str) -> Dict[str, Any]:
        """
        Fetch a single brewery by its ID.
        
        Args:
            brewery_id (str): The brewery ID
            
        Returns:
            dict: Brewery data
        """
        logger.info(f"Fetching brewery with ID: {brewery_id}")
        
        brewery = self._make_request(f"/breweries/{brewery_id}")
        
        return brewery
    
    def search_breweries(
        self,
        query: str,
        per_page: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search breweries by name, city, or state.
        
        Args:
            query (str): Search query
            per_page (int, optional): Number of results per page
            
        Returns:
            list: List of matching brewery dictionaries
        """
        params = {
            "query": query,
            "per_page": per_page or self.per_page
        }
        
        logger.info(f"Searching breweries with query: {query}")
        
        breweries = self._make_request("/breweries/search", params=params)
        
        logger.info(f"Found {len(breweries)} breweries matching query: {query}")
        
        return breweries
    
    def get_breweries_by_type(
        self,
        brewery_type: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch breweries filtered by type.
        
        Args:
            brewery_type (str): Type of brewery (e.g., 'micro', 'brewpub', 'regional')
            
        Returns:
            list: List of brewery dictionaries
        """
        params = {"by_type": brewery_type}
        
        logger.info(f"Fetching breweries of type: {brewery_type}")
        
        breweries = self._make_request("/breweries", params=params)
        
        logger.info(f"Found {len(breweries)} breweries of type: {brewery_type}")
        
        return breweries
    
    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
            logger.info("API client session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
