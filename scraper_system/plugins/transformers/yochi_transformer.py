import logging
import hashlib
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.models.location import TransformedLocation

logger = logging.getLogger(__name__)


class YochiTransformer(TransformerInterface):
    """
    Transformer for Yo-Chi data.
    Responsible for standardizing Google Maps data into the common QSR-Scraper format.
    """

    def _get_shopping_centre_patterns(self) -> List[str]:
        """Return patterns to identify shopping centers"""
        return [
            r"([\w\s]+Shopping Centre)",
            r"([\w\s]+Plaza)",
            r"([\w\s]+Square)",
            r"([\w\s]+Centre(?!\s+Shopping))",  # Centre but not followed by Shopping
            r"([\w\s]+Mall)",
            r"(Westfield[\s\w]+)",
            r"([\w\s]+Village)",
            r"([\w\s]+Emporium)",
            r"([\w\s]+Campus)",
            r"([\w\s]+Food Court)",
            r"([\w\s]+Waterside)",
            r"(Chadstone[\s\w]*)",  # Specific to major shopping centers
        ]

    def _extract_shopping_centre(self, address: str) -> Optional[str]:
        """
        Extract shopping centre name from address string.
        
        Args:
            address: The full address string
            
        Returns:
            Shopping centre name if found, None otherwise
        """
        if not address:
            return None
            
        patterns = self._get_shopping_centre_patterns()
        
        for pattern in patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                shopping_centre = match.group(1).strip()
                logger.debug(f"Extracted shopping centre: {shopping_centre}")
                return shopping_centre
        
        return None

    def _clean_address(self, address: str, shopping_centre: Optional[str] = None) -> str:
        """
        Clean the address by removing shopping centre references and normalizing format.
        
        Args:
            address: The original address
            shopping_centre: Shopping centre name to remove from address
            
        Returns:
            Cleaned address string
        """
        if not address:
            return ""
            
        cleaned = address.strip()
        
        # Remove shopping centre name from address if present
        if shopping_centre:
            # Remove the shopping centre name and common prefixes
            patterns_to_remove = [
                rf"\b{re.escape(shopping_centre)}\b",
                r"\bShop\s+[A-Z0-9/-]+\s*",
                r"\bUnit\s+[A-Z0-9/-]+\s*",
                r"\bSuite\s+[A-Z0-9/-]+\s*",
                r"^Shop\s+[A-Z0-9/-]+\s*[-,]?\s*",
                r"^Unit\s+[A-Z0-9/-]+\s*[-,]?\s*",
            ]
            
            for pattern in patterns_to_remove:
                cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
        
        # Clean up extra spaces and punctuation
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single
        cleaned = re.sub(r'^[,\s-]+|[,\s-]+$', '', cleaned)  # Leading/trailing punctuation
        cleaned = cleaned.strip()
        
        return cleaned

    def _normalize_state(self, state: str) -> Optional[str]:
        """
        Normalize state names to standard abbreviations.
        
        Args:
            state: State name or abbreviation
            
        Returns:
            Standardized state abbreviation
        """
        if not state:
            return None
            
        state_mapping = {
            'new south wales': 'NSW',
            'nsw': 'NSW',
            'victoria': 'VIC',
            'vic': 'VIC',
            'queensland': 'QLD',
            'qld': 'QLD',
            'south australia': 'SA',
            'sa': 'SA',
            'western australia': 'WA',
            'wa': 'WA',
            'tasmania': 'TAS',
            'tas': 'TAS',
            'northern territory': 'NT',
            'nt': 'NT',
            'australian capital territory': 'ACT',
            'act': 'ACT',
        }
        
        normalized = state_mapping.get(state.lower())
        if normalized:
            return normalized
        
        logger.warning(f"Unknown state: {state}")
        return state.upper()

    def _generate_business_id(self, business_name: str, address: str) -> str:
        """
        Generate a unique business ID based on name and address.
        
        Args:
            business_name: The business name
            address: The address
            
        Returns:
            Unique business ID hash
        """
        # Combine name and address for uniqueness
        combined = f"{business_name.lower().strip()}|{address.lower().strip()}"
        
        # Generate SHA-256 hash
        hash_object = hashlib.sha256(combined.encode('utf-8'))
        return hash_object.hexdigest()[:16]  # Use first 16 characters



    async def transform(
        self, data: List[Dict[str, Any]], config: Dict[str, Any], site_name: str
    ) -> List[Dict[str, Any]]:
        """
        Transform Yo-Chi location data into standardized format.
        
        Args:
            data: Raw location data from the parser
            config: Transformer configuration
            site_name: Name of the site being scraped
            
        Returns:
            List of transformed location dictionaries
        """
        logger.info(f"YochiTransformer starting transformation of {len(data)} items")
        
        transformed_locations = []
        
        for item in data:
            try:
                # Extract basic information
                brand = item.get('brand', 'Yo-Chi')
                business_name = item.get('business_name', '')
                raw_address = item.get('street_address', '')
                suburb = item.get('suburb', '')
                state = item.get('state', '')
                postcode = item.get('postcode', '')
                source_url = item.get('source_url', '')
                source = item.get('source', 'yochi')

                # Extract shopping centre information (for separate storage)
                shopping_centre = self._extract_shopping_centre(raw_address)

                # Remove "Yo-Chi" prefix from scraped address and keep as street_address
                street_address = raw_address
                if street_address.startswith("Yo-Chi "):
                    street_address = street_address[7:]  # Remove "Yo-Chi " prefix

                # Normalize state
                normalized_state = self._normalize_state(state)
                
                # Generate business ID using original scraped address
                business_id = self._generate_business_id(business_name, raw_address)
                
                # Validate essential fields
                if not business_name:
                    logger.warning(f"Skipping location with missing business name: {item}")
                    continue
                
                if not street_address and not suburb:
                    logger.warning(f"Skipping location with no address information: {business_name}")
                    continue
                
                # Create transformed location
                transformed_location = TransformedLocation(
                    brand=brand,
                    business_name=business_name,
                    street_address=street_address if street_address else None,
                    suburb=suburb if suburb else None,
                    state=normalized_state,
                    postcode=postcode if postcode else None,
                    drive_thru=False,  # Yo-Chi typically doesn't have drive-thru
                    shopping_centre_name=shopping_centre,
                    source_url=source_url if source_url else None,
                    source=source,
                    scraped_date=datetime.utcnow(),
                    status="ACTIVE",
                    business_id=business_id
                )
                
                # Convert to dictionary
                transformed_dict = transformed_location.dict()

                transformed_locations.append(transformed_dict)
                
                logger.debug(f"Transformed location: {business_name}")
                
            except Exception as e:
                logger.error(f"Error transforming location {item}: {e}")
                continue
        
        logger.info(f"YochiTransformer completed. Transformed {len(transformed_locations)} locations")
        return transformed_locations
