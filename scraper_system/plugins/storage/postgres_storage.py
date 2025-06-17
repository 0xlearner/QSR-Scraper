import logging
import os
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager
import asyncio
from collections import defaultdict

from sqlalchemy import Column, String, Boolean, DateTime, text, Integer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

# SQLAlchemy Base Model
Base = declarative_base()


# Memory buffer for storing data before database commits
class MemoryBuffer:
    def __init__(self, max_size=100):
        self.buffer = defaultdict(list)
        self.max_size = max_size

    def add(self, source, item):
        self.buffer[source].append(item)

    def get_and_clear(self, source=None):
        if source:
            items = self.buffer.get(source, [])
            self.buffer[source] = []
            return items
        else:
            all_items = []
            for src in self.buffer:
                all_items.extend(self.buffer[src])
            self.buffer.clear()
            return all_items

    def size(self, source=None):
        if source:
            return len(self.buffer.get(source, []))
        return sum(len(items) for items in self.buffer.values())

    def should_flush(self, source=None):
        if source:
            return len(self.buffer.get(source, [])) >= self.max_size
        return any(len(items) >= self.max_size for items in self.buffer.values())


class Location(Base):
    """SQLAlchemy ORM model for QSR Locations"""

    __tablename__ = "qsr_locations"  # Changed from "locations" to "qsr_locations"

    # Primary key will be business_id (hash of name+address)
    business_id = Column(String, primary_key=True)
    brand = Column(String, nullable=False)
    business_name = Column(String, nullable=False)
    street_address = Column(String)
    suburb = Column(String)
    state = Column(String)
    postcode = Column(String)
    drive_thru = Column(Boolean, default=False)
    shopping_centre_name = Column(String)
    source_url = Column(String)
    source = Column(String, nullable=False)
    scraped_date = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    status = Column(String, default="ACTIVE")  # ACTIVE, CLOSED, DUPLICATE
    closed_date = Column(DateTime, nullable=True)
    primary_record_id = Column(
        String, nullable=True
    )  # For duplicate records, points to the primary record
    last_seen_date = Column(DateTime, nullable=True)
    missing_count = Column(
        Integer, default=0
    )  # Track how many times location was missing

    def __repr__(self):
        return (
            f"<Location(business_id='{self.business_id}', "
            f"business_name='{self.business_name}', "
            f"address='{self.street_address}, {self.suburb} {self.state} {self.postcode}')>"
        )


class PostgresStorage(StorageInterface):
    """Stores data in a PostgreSQL database using SQLAlchemy ORM."""

    def __init__(self):
        """Initialize the Postgres storage handler."""
        self.engine = None
        self.async_session_factory = None
        self._is_initialized = False
        self.memory_buffer = MemoryBuffer(max_size=100)
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def get_session(self):
        """Create an async session context manager with proper error handling and rollback."""
        if not self.async_session_factory:
            raise ValueError("Database connection not initialized")

        session = self.async_session_factory()
        try:
            yield session
            await session.commit()
        except Exception as e:
            logger.error(f"Session error, rolling back: {str(e)}")
            await session.rollback()
            raise
        finally:
            await session.close()

    async def initialize_db(self, connection_string):
        """Initialize the database connection with Heroku Postgres optimized settings."""
        if self._is_initialized:
            return

        try:
            # Create an async engine with Heroku-optimized settings
            self.engine = create_async_engine(
                connection_string,
                echo=False,
                pool_pre_ping=True,  # Detect stale connections
                pool_size=5,  # Heroku has 20 connection limit, keep pool small
                max_overflow=2,  # Allow few extra connections
                pool_recycle=1800,  # Recycle connections every 30 minutes
                pool_timeout=30,  # Connection queue timeout
            )

            # Add retry logic with exponential backoff
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.async_session_factory = async_sessionmaker(
                        self.engine, expire_on_commit=False, class_=AsyncSession
                    )

                    # Verify connection and create tables
                    async with self.engine.begin() as conn:
                        # Test connection with simple query
                        await conn.execute(text("SELECT 1"))

                        # Check if tables exist
                        table_exists = await conn.run_sync(
                            lambda sync_conn: sync_conn.dialect.has_table(
                                sync_conn, "qsr_locations"
                            )
                        )

                        if not table_exists:
                            await conn.run_sync(Base.metadata.create_all)
                            logger.info("Created new tables in PostgreSQL database")
                        else:
                            logger.info("Tables already exist in PostgreSQL database")

                    self._is_initialized = True
                    logger.info("Successfully connected to Heroku Postgres database")
                    break

                except Exception as e:
                    retry_delay = (2**attempt) * 1.5  # Exponential backoff
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Database initialization attempt {attempt + 1} failed: {str(e)}. "
                            f"Retrying in {retry_delay} seconds..."
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error(
                            f"Failed to initialize Heroku Postgres after {max_retries} attempts: {e}"
                        )
                        raise

        except Exception as e:
            logger.error(f"Critical database initialization error: {e}", exc_info=True)
            raise

    def _get_connection_string(self, config: Dict[str, Any]) -> Optional[str]:
        """Get and format database connection string."""
        connection_string = config.get("connection_string") or os.environ.get(
            "DATABASE_URL"
        )
        if not connection_string:
            logger.error("No PostgreSQL connection string provided")
            return None

        # Mask password for logging
        log_string = connection_string
        if "@" in log_string:
            log_string = re.sub(r"://[^:]+:[^@]+@", "://***:***@", log_string)
        logger.debug(f"Attempting to connect with string: {log_string}")

        if connection_string.startswith("postgres://"):
            return connection_string.replace("postgres://", "postgresql+asyncpg://", 1)
        elif not connection_string.startswith("postgresql+asyncpg://"):
            return f"postgresql+asyncpg://{connection_string.split('://', 1)[1]}"
        return connection_string

    async def _deduplicate_data(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Deduplicate locations based on name and address."""
        unique_locations = {}

        for item in data:
            try:
                # Create composite key from cleaned address components
                components = [
                    "business_name",
                    "street_address",
                    "suburb",
                    "state",
                    "postcode",
                ]
                cleaned_values = [
                    (item.get(comp, "") or "").strip().lower() for comp in components
                ]
                location_key = "|".join(cleaned_values)

                # Keep record with more complete data
                if location_key in unique_locations:
                    existing = unique_locations[location_key]
                    if sum(1 for v in item.values() if v) > sum(
                        1 for v in existing.values() if v
                    ):
                        unique_locations[location_key] = item
                else:
                    unique_locations[location_key] = item
            except Exception as e:
                logger.error(f"Error deduplicating item: {e}", exc_info=True)
                continue

        return list(unique_locations.values())

    def _validate_location(self, item: Dict[str, Any]) -> bool:
        """Validate required location fields."""
        required_fields = {
            "business_id": item.get("business_id"),
            "business_name": item.get("business_name"),
            "street_address": (item.get("street_address") or "").strip(),
            "state": (item.get("state") or "").strip(),
            "suburb": (item.get("suburb") or "").strip(),
            "postcode": (item.get("postcode") or "").strip(),
        }

        if not all([required_fields["business_id"], required_fields["business_name"]]):
            logger.warning(
                f"Missing business_id or business_name: {required_fields['business_name'] or required_fields['business_id']}"
            )
            return False

        if not all(
            [
                required_fields["street_address"],
                required_fields["state"],
                required_fields["suburb"],
            ]
        ):
            logger.info(
                f"Incomplete address: Business: {required_fields['business_name']}, "
                f"Street: '{required_fields['street_address']}', "
                f"State: '{required_fields['state']}', "
                f"Suburb: '{required_fields['suburb']}'"
            )
            return False

        return True

    async def _handle_duplicates(self, session: AsyncSession, source: str):
        """Find and mark duplicate locations."""
        try:
            # Find groups of duplicates
            duplicates = await session.execute(
                text(
                    """
                    SELECT 
                        business_name, street_address, suburb, state, postcode,
                        array_agg(business_id) as business_ids,
                        array_agg(updated_at) as updated_ats
                    FROM qsr_locations
                    WHERE source = :source AND status = 'ACTIVE'
                    GROUP BY business_name, street_address, suburb, state, postcode
                    HAVING COUNT(*) > 1
                """
                ),
                {"source": source},
            )

            for group in duplicates.fetchall():
                # Keep most recently updated record as primary
                primary_id = group.business_ids[
                    group.updated_ats.index(max(group.updated_ats))
                ]
                duplicate_ids = [bid for bid in group.business_ids if bid != primary_id]

                if duplicate_ids:
                    await session.execute(
                        text(
                            """
                            UPDATE qsr_locations
                            SET status = 'DUPLICATE',
                                primary_record_id = :primary_id,
                                updated_at = NOW()
                            WHERE business_id = ANY(:duplicate_ids)
                        """
                        ),
                        {"primary_id": primary_id, "duplicate_ids": duplicate_ids},
                    )

        except Exception as e:
            logger.error(f"Error handling duplicates: {e}", exc_info=True)

    async def save(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        """Save data to memory buffer first, then flush to PostgreSQL if needed."""
        if not data:
            logger.info("No data provided to save to PostgreSQL.")
            return

        # Store the config for later use during close
        self.last_config = config

        try:
            # Get source from first item
            source = data[0].get("source", "unknown")

            # Process and deduplicate data
            deduplicated_data = await self._deduplicate_data(data)
            logger.info(
                f"Deduplicated {len(data)} locations to {len(deduplicated_data)} unique locations"
            )

            # Add valid locations to memory buffer
            valid_count = 0
            async with self.lock:
                for item in deduplicated_data:
                    if self._validate_location(item):
                        self.memory_buffer.add(source, item)
                        valid_count += 1

                logger.info(
                    f"Added {valid_count} valid locations to memory buffer for {source}"
                )

                # Check if we should flush the buffer
                if self.memory_buffer.should_flush(source):
                    await self._flush_buffer(source, config)

        except Exception as e:
            logger.error(f"Error saving data to memory buffer: {e}", exc_info=True)

    async def _flush_buffer(self, source: str, config: Dict[str, Any]):
        """Flush the memory buffer to the database."""
        items = self.memory_buffer.get_and_clear(source)
        if not items:
            return

        logger.info(
            f"Flushing {len(items)} items from memory buffer for {source} to database"
        )

        try:
            # Get and validate connection string
            connection_string = self._get_connection_string(config)
            if not connection_string:
                # Put items back in buffer if we can't connect
                for item in items:
                    self.memory_buffer.add(source, item)
                return

            # Initialize database connection if needed
            if not self._is_initialized:
                await self.initialize_db(connection_string)

            current_scrape_time = datetime.utcnow()
            current_business_ids = set()

            async with self.get_session() as session:
                # Save valid locations
                for item in items:
                    business_id = item["business_id"]
                    current_business_ids.add(business_id)

                    # Prepare location data with safe handling of None values
                    location_data = {
                        "business_id": business_id,
                        "brand": item.get("brand", ""),
                        "business_name": item["business_name"],
                        "street_address": (item.get("street_address") or "").strip(),
                        "suburb": (item.get("suburb") or "").strip(),
                        "state": (item.get("state") or "").strip(),
                        "postcode": (item.get("postcode") or "").strip(),
                        "drive_thru": bool(item.get("drive_thru", False)),
                        "shopping_centre_name": item.get("shopping_centre_name", ""),
                        "source_url": item.get("source_url", ""),
                        "source": item.get("source", source),
                        "scraped_date": item.get("scraped_date", current_scrape_time),
                    }

                    # Upsert location
                    stmt = insert(Location).values(**location_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["business_id"],
                        set_={
                            **location_data,
                            "updated_at": datetime.utcnow(),
                            "status": "ACTIVE",
                        },
                    )
                    await session.execute(stmt)

                # Handle duplicates and update statuses
                await self._handle_duplicates(session, source)
                await self._update_location_statuses(
                    session, source, current_business_ids, current_scrape_time
                )

            logger.info(
                f"Successfully flushed {len(items)} items to database for {source}"
            )

        except Exception as e:
            logger.error(f"Error flushing buffer to PostgreSQL: {e}", exc_info=True)
            # Put items back in buffer if flush fails
            for item in items:
                self.memory_buffer.add(source, item)

    async def _update_location_statuses(
        self,
        session: AsyncSession,
        source: str,
        current_business_ids: set,
        current_scrape_time: datetime,
    ):
        """Update location statuses based on current scrape."""
        try:
            business_ids_list = list(current_business_ids)

            # Update seen locations
            await session.execute(
                text(
                    """
                    UPDATE qsr_locations 
                    SET last_seen_date = :current_time,
                        missing_count = 0
                    WHERE source = :source 
                    AND business_id = ANY(:business_ids)
                    AND status = 'ACTIVE'
                """
                ),
                {
                    "source": source,
                    "business_ids": business_ids_list,
                    "current_time": current_scrape_time,
                },
            )

            # Update missing locations
            await session.execute(
                text(
                    """
                    UPDATE qsr_locations 
                    SET missing_count = missing_count + 1
                    WHERE source = :source 
                    AND business_id != ALL(:business_ids)
                    AND status = 'ACTIVE'
                """
                ),
                {"source": source, "business_ids": business_ids_list},
            )

            # Mark as closed after 20 missing occurrences
            await session.execute(
                text(
                    """
                    UPDATE qsr_locations 
                    SET status = 'CLOSED',
                        closed_date = :closed_date
                    WHERE source = :source 
                    AND missing_count >= 20
                    AND status = 'ACTIVE'
                """
                ),
                {"source": source, "closed_date": current_scrape_time},
            )

        except Exception as e:
            logger.error(f"Error updating location statuses: {e}", exc_info=True)

    async def close(self):
        """Close database connections and flush any remaining data."""
        try:
            # Flush any remaining data in the buffer
            if self.memory_buffer.size() > 0:
                logger.info(
                    f"Flushing remaining {self.memory_buffer.size()} items before closing"
                )
                # Store the last used config to reuse during close
                if hasattr(self, "last_config"):
                    for source in list(self.memory_buffer.buffer.keys()):
                        if self.memory_buffer.size(source) > 0:
                            await self._flush_buffer(source, self.last_config)
                else:
                    logger.warning("No configuration available for final flush")

            # Close database connections
            if self.engine:
                await self.engine.dispose()
                self._is_initialized = False
                logger.info("PostgreSQL database connection closed")
        except Exception as e:
            logger.error(f"Error during PostgreSQL close: {e}", exc_info=True)
