from pydantic import BaseModel, model_validator
from datetime import datetime
from typing import Optional, Dict, Any
import hashlib
import json

class EventCreate(BaseModel):
    """Модель для создания события с динамическими полями"""
    event_name: Optional[str] = None
    event_datetime: Optional[datetime] = None
    # Все остальные поля как опциональные
    # ...

    @model_validator(mode='after')
    def generate_unique_hash(self):
        """Генерация уникального хэша на основе всех полей"""
        data = self.model_dump(exclude_unset=True, exclude_none=True)
        data_str = json.dumps(data, sort_keys=True)
        self.event_hash = hashlib.sha256(data_str.encode()).hexdigest()
        return self

class EventResponse(EventCreate):
    """Модель ответа с системными полями"""
    id: int
    created_at: datetime
    raw_data: Dict[str, Any]

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }