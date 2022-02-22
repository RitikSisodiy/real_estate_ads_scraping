from pydantic import BaseModel
from enum import Enum
from typing import Optional


class RealStateParameter(BaseModel):
    text: Optional[str] = ''
    min_price: Optional[float] = 0
    max_price: Optional[float] = 0
    city: Optional[str] = ''
    rooms: Optional[int] = 0


class VendorType(str, Enum):
    individuals = "individuals"
    professionals = "professionals"


class RealStateType(str, Enum):
    sale = "sale"
    rental = "rental"


class PropertyType(str, Enum):
    house = "house"
    apartment = "apartment"
    ground = "ground"
    garage_parking = ""
