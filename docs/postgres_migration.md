# Migrating to PostgreSQL Storage

This guide explains how to migrate your QSR Scraper System from JSON storage to PostgreSQL storage, which is particularly useful when deploying to Heroku.

## Prerequisites

1. Make sure you have the required dependencies:
   ```
   sqlalchemy>=2.0.0
   asyncpg>=0.29.0
   greenlet>=2.0.2
   ```

2. Access to a PostgreSQL database (such as Heroku Postgres)

## Configuration Steps

### 1. Install the Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Your PostgreSQL Database

#### On Heroku:

```bash
# Add the PostgreSQL add-on to your Heroku app
heroku addons:create heroku-postgresql:mini

# Verify the DATABASE_URL is set
heroku config:get DATABASE_URL
```

#### Locally (for development):

Create a PostgreSQL database and set the DATABASE_URL environment variable:

```bash
export DATABASE_URL="postgresql+asyncpg://username:password@localhost:5432/database_name"
```

### 3. Update Your Configuration

Edit your `configs/config.yaml` file to include PostgresStorage in the storage section for each website:

```yaml
storage:
  - JSONStorage
  - PostgresStorage
config:
  storage_options:
    JSONStorage:
      output_file: "data/locations.jsonl"
    PostgresStorage:
      connection_string: "${DATABASE_URL}"
```

### 4. Run the Scraper

The database tables will be automatically created on the first run:

```bash
python main.py
```

## Data Migration

If you have existing JSON data that you want to migrate to PostgreSQL:

1. Create a migration script that:
   - Reads your existing JSONL files
   - Connects to your PostgreSQL database
   - Inserts the data using the same schema

Example migration script outline:

```python
import asyncio
import json
import os
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from scraper_system.plugins.storage.postgres_storage import Base, Location

async def migrate_json_to_postgres():
    # Get database URL from environment
    db_url = os.environ.get('DATABASE_URL')
    if db_url and db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+asyncpg://', 1)
    
    # Create engine and session
    engine = create_async_engine(db_url)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    
    # Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Read JSON files and insert into database
    json_files = [
        "data/grilld_locations.jsonl",
        "data/gyg_locations.jsonl",
        "data/kfc_locations.jsonl",
        "data/eljannah_locations.jsonl",
        "data/noodlebox_locations.jsonl"
    ]
    
    for json_file in json_files:
        if not os.path.exists(json_file):
            print(f"File not found: {json_file}")
            continue
            
        print(f"Migrating data from {json_file}...")
        with open(json_file, 'r') as f:
            async with async_session() as session:
                for line in f:
                    data = json.loads(line.strip())
                    location = Location(
                        business_id=data.get('business_id'),
                        business_name=data.get('business_name'),
                        street_address=data.get('street_address'),
                        suburb=data.get('suburb'),
                        state=data.get('state'),
                        postcode=data.get('postcode'),
                        drive_thru=data.get('drive_thru', False),
                        shopping_centre_name=data.get('shopping_centre_name'),
                        source_url=data.get('source_url'),
                        source=data.get('source'),
                        scraped_date=data.get('scraped_date', datetime.utcnow()),
                    )
                    session.add(location)
                await session.commit()
        print(f"Migration complete for {json_file}")

if __name__ == "__main__":
    asyncio.run(migrate_json_to_postgres())
```

Save this as `migrate_data.py` and run with:

```bash
python migrate_data.py
```

## Verifying the Migration

You can check if your data was properly stored in PostgreSQL by using a database client or running SQL queries:

```sql
SELECT COUNT(*) FROM locations;
SELECT * FROM locations LIMIT 10;
```

## Troubleshooting

1. **Connection errors**: Ensure your DATABASE_URL is correctly formatted for asyncpg
2. **Missing data**: Check that business_id is being generated for all records
3. **Slow performance**: Consider batching inserts for large datasets