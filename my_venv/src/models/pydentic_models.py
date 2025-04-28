from pydantic import BaseModel
from pydantic import ConfigDict, Field, ValidationError, field_validator, model_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import json

from my_venv.src.services.event_hashing import EventHashService, HashGenerationError
from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import DataNormalizer


class EventBase(BaseModel):
    discount_items_ids: Optional[List[str]] = Field(default_factory=list)
    experiments: Optional[List[str]] = Field(default_factory=list)
    event_name: str = "default_event"
    event_hash: Optional[str] = Field(
        None,
        pattern=r'^[a-f0-9]{64}$',
        examples=["a1b2c3..."]
    )

    model_config = ConfigDict(
        # Разрешаем любые дополнительные поля
        extra="allow",
        validate_assignment=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        str_min_length=0
    )

    @classmethod
    @field_validator("experiments", "discount_items_ids", mode="before")
    def parse_json_fields(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format")
        elif isinstance(value, list):
            return value
        else:
            raise ValueError("Invalid data format")

    def to_normalized_dict(self) -> Dict[str, Any]:
        """Нормализация данных для хеширования"""
        return DataNormalizer.deep_clean(
            self.model_dump(
                exclude={'event_hash'},
                exclude_none=True,
                by_alias=True
            )
        )

class EventCreate(EventBase):

    @model_validator(mode='after')
    def generate_event_hash(self) -> 'EventCreate':
        if self.event_hash is None:
            try:
                normalized = self.to_normalized_dict()
                self.event_hash = EventHashService.generate_unique_fingerprint(
                    data=normalized,
                    event_name=self._detect_event_type(normalized)
                )
            except HashGenerationError as e:
                logger.error(f"Hash generation failed: {e}")
                raise ValidationError("Event validation error") from e
        return self

    @staticmethod
    def _detect_event_type(data: Dict[str, Any]) -> str:
        return data.get('event_type') or f"action_{data.get('action')}" or "default_event"

class EventResponse(EventCreate):
    status: str = "success"
    timestamp: datetime = Field(default_factory=datetime.now)
    duplicate_fields: Optional[List[str]] = None
    processing_id: Optional[str] = Field(
        None,
        description="Идентификатор обработки события в очереди"
    )