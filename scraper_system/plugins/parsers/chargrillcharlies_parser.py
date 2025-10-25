import logging
import json
from typing import List, Dict, Any, Optional
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.fetcher_interface import FetcherInterface

logger = logging.getLogger(__name__)


class ChargrillCharliesParser(ParserInterface):
    """
    Parser for Chargrill Charlie's locations using zendriver to extract NUXT data.
    Extracts location data from the Apollo GraphQL cache in the __NUXT__ object.
    """

    def __init__(self, fetcher: Optional[FetcherInterface] = None):
        if fetcher is None:
            raise ValueError("ChargrillCharliesParser requires a Fetcher instance.")
        self.fetcher = fetcher
        logger.info("ChargrillCharliesParser initialized with a fetcher.")

    async def parse(
        self, content: str, content_type: Optional[str], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parse Chargrill Charlie's locations by extracting NUXT data.
        
        This parser ignores the initial content and directly fetches the locations page
        to extract data from the __NUXT__ object using JavaScript execution.
        """
        logger.info("ChargrillCharliesParser starting NUXT data extraction.")
        
        # The URL to fetch - we ignore the initial content and fetch directly
        url = "https://chargrillcharlies.com/locations"
        
        # Configure the fetcher to extract NUXT data
        fetcher_config = config.get("fetcher_options", {})
        
        # JavaScript code to extract and resolve location data
        extract_js = """
            (() => {
                if (!window.__NUXT__) return null;
                
                const nuxt = window.__NUXT__;
                let storeReferences = [];
                let resolvedStores = [];
                
                // Get Apollo cache
                const apollo = nuxt.apollo?.defaultClient;
                if (!apollo) return null;
                
                // Helper function to resolve Apollo references recursively
                function resolveReference(obj, apollo, visited = new Set()) {
                    if (!obj || typeof obj !== 'object') return obj;
                    
                    // Handle Apollo reference objects
                    if (obj.id && obj.type === 'id' && apollo[obj.id]) {
                        if (visited.has(obj.id)) return { __ref: obj.id }; // Prevent circular references
                        visited.add(obj.id);
                        return resolveReference(apollo[obj.id], apollo, visited);
                    }
                    
                    // Handle arrays
                    if (Array.isArray(obj)) {
                        return obj.map(item => resolveReference(item, apollo, visited));
                    }
                    
                    // Handle objects
                    const resolved = {};
                    for (const [key, value] of Object.entries(obj)) {
                        resolved[key] = resolveReference(value, apollo, visited);
                    }
                    return resolved;
                }
                
                // Find store references in ROOT_QUERY
                if (apollo.ROOT_QUERY) {
                    for (const [key, value] of Object.entries(apollo.ROOT_QUERY)) {
                        if (key.includes('entries') && key.includes('stores')) {
                            storeReferences = value;
                            break;
                        }
                    }
                }
                
                // Resolve references to complete store data
                if (storeReferences.length > 0) {
                    for (const ref of storeReferences) {
                        if (ref && ref.id && apollo[ref.id]) {
                            const storeData = apollo[ref.id];
                            // Fully resolve all nested references
                            const resolvedStore = resolveReference(storeData, apollo);
                            resolvedStores.push(resolvedStore);
                        }
                    }
                }
                
                return {
                    count: resolvedStores.length,
                    stores: resolvedStores
                };
            })()
        """
        
        # Update fetcher config to extract NUXT data
        fetcher_config.update({
            "wait_for_load": 3.0,  # Wait for NUXT to load
            "extract_data": extract_js  # Extract the data we need
        })
        
        try:
            # Fetch the page and extract NUXT data
            logger.info(f"Fetching and extracting data from {url}")
            extracted_data, content_type, status_code = await self.fetcher.fetch(url, fetcher_config)
            
            if not extracted_data:
                logger.error("No data extracted from Chargrill Charlie's website")
                return []
            
            # Parse the extracted JSON data
            try:
                nuxt_data = json.loads(extracted_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse extracted data as JSON: {e}")
                return []
            
            if not nuxt_data or not isinstance(nuxt_data, dict):
                logger.error("Invalid NUXT data structure")
                return []
            
            stores = nuxt_data.get('stores', [])
            logger.info(f"Found {len(stores)} stores in NUXT data")
            
            # Process each store into the expected format
            locations = []
            for store in stores:
                location_data = self._process_store(store)
                if location_data:
                    locations.append(location_data)
            
            logger.info(f"ChargrillCharliesParser finished, returning {len(locations)} items.")
            return locations
            
        except Exception as e:
            logger.error(f"Error in ChargrillCharliesParser: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _process_store(self, store: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single store from the NUXT data into the expected format.
        """
        try:
            # Extract basic information
            title = store.get('title', '')
            description = store.get('store_description', '')
            phone = store.get('store_phoneNumber', '')
            google_maps_link = store.get('store_googleMapsLink', '')
            
            # Extract location information
            location = store.get('store_location', {})
            latitude = location.get('lat')
            longitude = location.get('lng')
            parts = location.get('parts', {})
            
            # Extract features
            features_data = store.get('store_features', {})
            features = []
            if features_data and features_data.get('type') == 'json':
                features = features_data.get('json', [])
            
            # Build address components
            street_number = parts.get('number', '')
            street_name = parts.get('address', '')
            suburb = parts.get('city', '')
            state = parts.get('state', '')
            postcode = parts.get('postcode', '')
            country = parts.get('country', '')
            
            # Build full street address
            street_address_parts = []
            if street_number:
                street_address_parts.append(str(street_number))
            if street_name:
                street_address_parts.append(street_name)
            street_address = ' '.join(street_address_parts) if street_address_parts else ''
            
            # Determine if this is a drive-thru location
            drive_thru = 'drivethru' in features or 'drive-thru' in features or 'drive_thru' in features
            
            # Create the location data in the expected format
            location_data = {
                "brand": "Chargrill Charlie's",
                "business_name": f"Chargrill Charlie's {title}",
                "street_address": street_address,
                "suburb": title,  # Use the title as suburb (e.g., "Annandale")
                "state": state,
                "postcode": postcode,
                "country": country,
                "latitude": latitude,
                "longitude": longitude,
                "phone": phone,
                "google_maps_link": google_maps_link,
                "features": features,
                "drive_thru": drive_thru,
                "description": description,
                "source_url": "https://chargrillcharlies.com/locations",
                "source": "chargrillcharlies",
            }
            
            # Only return if we have essential data
            if title and (street_address or latitude):
                return location_data
            else:
                logger.warning(f"Skipping store with insufficient data: {title}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing store data: {e}")
            return None
