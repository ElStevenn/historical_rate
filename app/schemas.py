from pydantic import BaseModel
from typing import Optional, List, Literal
from datetime import datetime




# RESPONSE SCHEMA   
class Crypto(BaseModel):
    symbol: str
    name: Optional[str]
    image: Optional[str]
    description: Optional[str]
    funding_rate_delay: Literal['8h', '4h']
    next_execution_time: datetime

class CryptoSearch(BaseModel):
    id: int
    symbol: str
    name: Optional[str]
    image: Optional[str]