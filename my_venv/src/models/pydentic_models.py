import json
from pydantic import BaseModel, model_validator, ConfigDict, field_validator, Field
from datetime import datetime
from typing import Optional

from my_venv.src.services.event_hashing import EventHashService
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
    model_config = {
        "extra": "allow",
        "validate_default": True
    }

    @model_validator(mode='after')
    def generate_event_hash(self) -> 'DynamicEvent':
        # Нормализация данных
        data = self.model_dump(
            exclude={'event_hash'},
            exclude_none=True,
            exclude_unset=True,
            by_alias=True
        )

        # Сортировка и сериализация
        sorted_data = dict(sorted(data.items()))
        payload = json.dumps(
            sorted_data,
            separators=(',', ':'),
            default=self._serializer
        )

        # Генерация хеша
        self.event_hash = hashlib.sha256(payload.encode()).hexdigest()
        return self

    def _serializer(self, obj: Any) -> str:
        """Кастомная сериализация для специальных типов"""
        if isinstance(obj, set):
            return json.dumps(sorted(obj))
        if hasattr(obj, 'dict'):
            return obj.dict()
        return str(obj)
