from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field

class TransformedLocation(BaseModel):
    """
    Standardized data structure for a scraped QSR location after transformation.
    This model serves as the common output format for all transformers.
    """
    business_name: str
    street_address: Optional[str] = None
    suburb: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    drive_thru: bool = False # Default to False if not specified by parser/transformer
    shopping_centre_name: Optional[str] = None
    source_url: Optional[str] = None # URL of the specific location page
    source: str # Identifier for the source website (e.g., 'grilld', 'kfc')
    scraped_date: datetime = Field(default_factory=datetime.utcnow) # Automatically set UTC timestamp
    business_id: Optional[str] = None # Unique ID (e.g., hash of name+address)

    class Config:
        # Example: If needed later for ORM mode or other configurations
        # orm_mode = True
        pass
