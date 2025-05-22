import logging
import os
from typing import List, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

from sqlalchemy import Column, String, Boolean, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

# SQLAlchemy Base Model
Base = declarative_base()


class Location(Base):
    """SQLAlchemy ORM model for QSR Locations"""
    __tablename__ = 'locations'

    # Primary key will be business_id (hash of name+address)
    business_id = Column(String, primary_key=True)
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

    def __repr__(self):
        return (f"<Location(business_id='{self.business_id}', "
                f"business_name='{self.business_name}', "
                f"address='{self.street_address}, {self.suburb} {self.state} {self.postcode}')>")


class PostgresStorage(StorageInterface):
    """Stores data in a PostgreSQL database using SQLAlchemy ORM."""

    def __init__(self):
        """Initialize the Postgres storage handler."""
        self.engine = None
        self.async_session_factory = None

    @asynccontextmanager
    async def get_session(self):
        """Create an async session context manager."""
        if not self.async_session_factory:
            raise ValueError("Database connection not initialized")

        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                raise e

    async def initialize_db(self, connection_string):
        """Initialize the database connection and create tables if they don't exist."""
        try:
            # Create an async engine
            self.engine = create_async_engine(
                connection_string,
                echo=False,  # Set to True for debugging SQL queries
                pool_pre_ping=True,  # Verify connection before using from pool
                pool_size=5,  # Reasonable pool size for most workloads
                max_overflow=10  # Allow up to 10 additional connections when pool is full
            )

            # Create a session factory
            self.async_session_factory = async_sessionmaker(
                self.engine,
                expire_on_commit=False,
                class_=AsyncSession
            )

            # Create tables if they don't exist
            async with self.engine.begin() as conn:
                # Create all tables defined in Base's metadata
                await conn.run_sync(Base.metadata.create_all)

            logger.info("PostgreSQL database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing PostgreSQL database: {e}", exc_info=True)
            raise

    async def save(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        """
        Save data to PostgreSQL database.

        Args:
            data: List of transformed location dictionaries to save
            config: Configuration with connection details
                - connection_string: SQLAlchemy PostgreSQL connection string
                    e.g., 'postgresql+asyncpg://username:password@hostname/database'
        """
        if not data:
            logger.info("No data provided to save to PostgreSQL.")
            return

        # Get the connection string from config
        connection_string = config.get('connection_string')
        if not connection_string:
            # Try to get it from environment variable if not in config
            connection_string = os.environ.get('DATABASE_URL')
            if not connection_string:
                logger.error("No PostgreSQL connection string provided in config or DATABASE_URL environment variable")
                return

            # Fix Heroku's DATABASE_URL format if needed (convert postgres:// to postgresql://)
            if connection_string.startswith('postgres://'):
                connection_string = connection_string.replace('postgres://', 'postgresql+asyncpg://', 1)
            elif not connection_string.startswith('postgresql+asyncpg://'):
                connection_string = f"postgresql+asyncpg://{connection_string.split('://', 1)[1]}"

        # Initialize database if not already done
        if not self.engine:
            try:
                await self.initialize_db(connection_string)
            except Exception as e:
                logger.error(f"Failed to initialize database: {e}")
                return

        # Process and save each location
        try:
            async with self.get_session() as session:
                for item in data:
                    # Convert dictionary to Location ORM object
                    location_data = {
                        'business_id': item.get('business_id'),
                        'business_name': item.get('business_name'),
                        'street_address': item.get('street_address'),
                        'suburb': item.get('suburb'),
                        'state': item.get('state'),
                        'postcode': item.get('postcode'),
                        'drive_thru': item.get('drive_thru', False),
                        'shopping_centre_name': item.get('shopping_centre_name'),
                        'source_url': item.get('source_url'),
                        'source': item.get('source'),
                        'scraped_date': item.get('scraped_date', datetime.utcnow())
                    }

                    # Skip items without business_id (required as primary key)
                    if not location_data['business_id']:
                        logger.warning(f"Skipping item with missing business_id: {item.get('business_name')}")
                        continue

                    # Create an upsert statement for this location
                    stmt = insert(Location).values(**location_data)

                    # Add an ON CONFLICT clause to update existing records
                    # This is the PostgreSQL "upsert" feature
                    update_dict = {k: v for k, v in location_data.items() if k != 'business_id'}
                    update_dict['updated_at'] = datetime.utcnow()

                    stmt = stmt.on_conflict_do_update(
                        index_elements=['business_id'],
                        set_=update_dict
                    )

                    await session.execute(stmt)

                await session.commit()
                logger.info(f"Successfully saved {len(data)} locations to PostgreSQL database")

        except Exception as e:
            logger.error(f"Error saving data to PostgreSQL: {e}", exc_info=True)
            raise

    async def close(self):
        """Close database connections when shutting down."""
        if self.engine:
            await self.engine.dispose()
            logger.info("PostgreSQL database connections closed")
