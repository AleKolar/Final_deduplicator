import json
from pydantic import BaseModel, model_validator, ConfigDict, field_validator, Field
from datetime import datetime
from typing import Optional, Dict, Any

from my_venv.src.services.event_hashing import EventHashService, HashGenerationError
from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import JsonSerializer, DataNormalizer


class EventBase(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        str_min_length=0  # Разрешаем пустые строки
    )
    @classmethod
    @field_validator("experiments", "discount_items_ids", "discount_items_names", mode="before")
    def parse_json_fields(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(JsonSerializer.serialize(value))
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format")
        return value

class EventCreate(EventBase):
    event_hash: Optional[str] = Field(
        None,
        pattern=r'^[a-f0-9]{64}$',
        examples=["a1b2c3..."]
    )

    @model_validator(mode='after')
    def generate_event_hash(self) -> 'EventCreate':
        if self.event_hash is None:
            data = self.model_dump(
                exclude={'event_hash'},
                exclude_none=True,
                by_alias=True
            )
            normalized = DataNormalizer.deep_clean(data)
            self.event_hash = EventHashService.generate_unique_fingerprint(
                data=normalized,
                event_name=self.get_event_name()
            )
        return self

    def get_event_name(self) -> str:
        return getattr(self, "event_type", "default_event")

class EventResponse(EventCreate):
    status: str = "success"
    timestamp: datetime = Field(default_factory=datetime.now)


class DynamicEvent(BaseModel):
    event_hash: Optional[str] = Field(
        None,
        pattern=r"^[a-f0-9]{64}$",
    )

    model_config = {
        "extra": "allow",
        "validate_default": True,
        "str_strip_whitespace": True
    }

    @model_validator(mode="after")
    def generate_event_hash(self) -> "DynamicEvent":
        if self.event_hash is not None:
            return self

        try:
            raw_data = self.model_dump(
                exclude={"event_hash"},
                exclude_none=True,
                exclude_unset=True,
                by_alias=True
            )

            event_name = self._detect_event_type(raw_data)

            self.event_hash = EventHashService.generate_unique_fingerprint(
                data=raw_data,
                event_name=event_name
            )
            return self

        except HashGenerationError as e:
            logger.error(f"Hash generation failed: {e}")
            raise ValueError("Event validation error") from e

    @staticmethod
    def _detect_event_type(data: Dict[str, Any]) -> str:
        """Определение типа события без доступа к экземпляру"""
        if 'event_type' in data:
            return str(data['event_type'])
        if 'action' in data:
            return f"action_{data['action']}"
        return "default_event"