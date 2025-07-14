# Nandos Parser Documentation

## Overview

The Nandos parser is designed to scrape restaurant location data from the Nandos Australia website (https://www.nandos.com.au/restaurants). It implements a three-phase scraping strategy to comprehensively collect all restaurant locations across Australia.

## Architecture

### Three-Phase Scraping Process

1. **Phase 1: State/Territory Discovery**
   - Parses the main restaurants page (`https://www.nandos.com.au/restaurants`)
   - Extracts links to state/territory pages (e.g., `/restaurants/nsw`, `/restaurants/qld`)
   - Converts relative URLs to absolute URLs

2. **Phase 2: Restaurant URL Collection**
   - Fetches each state/territory page concurrently
   - Extracts individual restaurant URLs from each state page
   - Deduplicates URLs to avoid processing the same restaurant multiple times

3. **Phase 3: Restaurant Data Extraction**
   - Fetches each restaurant detail page
   - Extracts structured JSON-LD data containing restaurant information
   - Specifically looks for `@type: "Restaurant"` schema data

## Data Extraction

### Source Data Format
The parser extracts data from JSON-LD script tags with the following structure:
```json
{
  "@context": "https://schema.org",
  "@type": "Restaurant",
  "@id": "70",
  "name": "FOREST LAKE SHOPPING CENTRE",
  "url": "https://www.nandos.com.au/restaurants/qld/forest-lake-shopping-centre",
  "telephone": "07 3279 8317",
  "address": {
    "@type": "PostalAddress",
    "streetAddress": "Shopping Centre, Shop 34/235 Forest Lake Blvd",
    "addressLocality": "Forest Lake",
    "addressRegion": "QLD",
    "postalCode": "4078",
    "addressCountry": "AUSTRALIA"
  },
  "geo": {
    "@type": "GeoCoordinates",
    "latitude": -27.625403,
    "longitude": 152.967462
  },
  "openingHoursSpecification": [...]
}
```

### Extracted Fields
- **name**: Restaurant name
- **address**: Complete postal address with components
- **telephone**: Phone number
- **geo**: Latitude and longitude coordinates
- **openingHoursSpecification**: Operating hours by day
- **url**: Official restaurant page URL

## Configuration

### Basic Configuration
```yaml
nandos:
  enabled: true
  start_urls:
    - https://www.nandos.com.au/restaurants
  fetcher: AsyncHTTPXFetcher
  parser: NandosParser
  transformer: NandosTransformer
  storage: [PostgresStorage]
```

### Fetcher Options
```yaml
config:
  fetcher_options:
    headers:
      User-Agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    timeout: 30

  # Options for detail page fetches (state pages and restaurant pages)
  detail_fetcher_options:
    timeout: 45
    headers:
      User-Agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
```

## Data Transformation

The `NandosTransformer` processes the raw JSON-LD data and converts it to the standardized `TransformedLocation` format:

### Address Processing
- Extracts street address, suburb, state, and postcode from the structured address
- Identifies and separates shopping centre names from street addresses
- Normalizes state codes (e.g., "Queensland" → "QLD")
- Validates and formats postcodes (4-digit Australian format)

## Output Format

The transformer produces standardized location records:
```json
{
  "brand": "Nandos",
  "business_name": "FOREST LAKE SHOPPING CENTRE",
  "street_address": "Shop 34/235 Forest Lake Blvd",
  "suburb": "Forest Lake",
  "state": "QLD",
  "postcode": "4078",
  "drive_thru": false,
  "shopping_centre_name": "Shopping Centre",
  "source_url": "https://www.nandos.com.au/restaurants/qld/forest-lake-shopping-centre",
  "source": "nandos",
  "business_id": "a1b2c3d4e5f6...",
  }
}
```

## Error Handling

### Parser Error Handling
- Gracefully handles missing or malformed HTML selectors
- Continues processing if individual state pages fail
- Logs warnings for restaurants without JSON-LD data
- Returns partial results if some restaurants fail to parse

### Transformer Error Handling
- Validates address components before processing
- Handles missing or invalid coordinate data
- Continues processing if individual records fail transformation
- Logs detailed error information for debugging

## Performance Considerations

### Concurrency
- State pages are fetched concurrently to improve performance
- Restaurant detail pages are fetched concurrently with controlled parallelism
- Uses `asyncio.gather()` for efficient async processing

### Rate Limiting
- Configurable timeouts for different request types
- Respects server response times with appropriate delays
- Separate timeout settings for initial, state, and detail page requests

## Monitoring and Debugging

### Logging
The parser provides detailed logging at multiple levels:
- `INFO`: High-level progress and summary statistics
- `DEBUG`: Individual URL processing and data extraction details
- `WARNING`: Missing data or parsing issues that don't stop processing
- `ERROR`: Critical failures that prevent data extraction

### Key Metrics to Monitor
- Number of state URLs discovered
- Number of restaurant URLs collected
- Success rate of restaurant page parsing
- Transformation success rate
- Total processing time

## Maintenance Notes

### Potential Breaking Changes
1. **HTML Structure Changes**: The parser relies on specific CSS selectors for the main restaurants page
2. **JSON-LD Schema Changes**: Changes to the Restaurant schema structure could affect data extraction
3. **URL Pattern Changes**: Changes to the URL structure (`/restaurants/state/restaurant-name`) could break URL collection

### Selector Dependencies
- Main container: `.sc-d614dbc0-0.sc-93a5572f-0.sc-fdb88f63-0.fhoMFm.dSqgfi.hFLYxk`
- State links: `a[href^='/restaurants/']`
- JSON-LD scripts: `script[type="application/ld+json"]`

### Recommended Monitoring
- Monitor for changes in the number of locations discovered
- Set up alerts for significant drops in parsing success rates
- Regularly validate that the JSON-LD schema hasn't changed
