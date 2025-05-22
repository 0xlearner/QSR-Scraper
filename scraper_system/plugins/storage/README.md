# QSR Scraper System - Storage Plugins

This directory contains the storage plugins for the QSR Scraper System. These plugins are responsible for persisting scraped data to various storage backends.

## Available Storage Plugins

### JSONStorage

The `JSONStorage` plugin stores scraped location data as JSON Lines (JSONL) in files. Each location is stored as a separate JSON object on its own line.

### PostgresStorage

The `PostgresStorage` plugin stores scraped location data in a PostgreSQL database using SQLAlchemy ORM. This is particularly useful for Heroku deployments which often use Postgres as their database.

## PostgreSQL Storage Configuration

To use the PostgreSQL storage plugin, you need to:

1. Add `PostgresStorage` to the `storage` list in your config.yaml for each website
2. Configure the database connection string in the `storage_options` section

Example configuration:

```yaml
storage:
  - JSONStorage
  - PostgresStorage
config:
  storage_options:
    PostgresStorage:
      connection_string: "${DATABASE_URL}"  # Use environment variable
```

### Heroku PostgreSQL Setup

For Heroku deployments:

1. Add the PostgreSQL add-on to your Heroku app:
   ```
   heroku addons:create heroku-postgresql:mini
   ```

2. The `DATABASE_URL` environment variable will be automatically set by Heroku

3. The database schema will be automatically created on first run

### Database Schema

The PostgreSQL storage uses a `locations` table with the following schema:

- `business_id` (Primary Key): A unique identifier (hash of name+address)
- `business_name`: Name of the business location
- `street_address`: Street address of the location
- `suburb`: Suburb/city name
- `state`: State or territory code
- `postcode`: Postal code
- `drive_thru`: Boolean indicating if the location has a drive-thru
- `shopping_centre_name`: Name of the shopping center (if applicable)
- `source_url`: URL of the location on the source website
- `source`: Identifier for the source website (e.g., 'kfc_au')
- `scraped_date`: Timestamp when the location was scraped
- `created_at`: Timestamp when the record was first created
- `updated_at`: Timestamp when the record was last updated

## Dependencies

To use the PostgreSQL storage, you need to install:

```
sqlalchemy>=2.0.0
asyncpg>=0.29.0
greenlet>=2.0.2
```

These are included in the project's requirements.txt file.