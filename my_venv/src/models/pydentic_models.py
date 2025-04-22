import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, model_validator, field_validator


class EventBase(BaseModel):
    event_hash: Optional[str] = None
    event_name: Optional[str] = None
    event_datetime: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    profile_id: Optional[str] = None
    device_ip: Optional[str] = None
    raw_data: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(
        extra='ignore',
        populate_by_name=True
    )

    @model_validator(mode='before')
    @classmethod
    def prepare_data(cls, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict):
            # Собираем все дополнительные поля
            model_fields = cls.model_fields.keys()
            extra_fields = {k: v for k, v in data.items() if k not in model_fields}

            # Объединяем с существующим raw_data
            raw_data = {**data.get('raw_data', {}), **extra_fields}

            # Фильтруем только разрешенные поля
            filtered_data = {k: v for k, v in data.items() if k in model_fields}

            return {
                **filtered_data,
                'raw_data': raw_data,
                'created_at': data.get('created_at', datetime.now(timezone.utc))
            }
        return data

    @field_validator('event_datetime', 'created_at', mode='before')
    @classmethod
    def parse_datetime(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except (TypeError, ValueError):
            return datetime.now(timezone.utc)


class EventCreate(EventBase):
    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()}
    )

    @field_validator('event_hash', mode='before')
    @classmethod
    def generate_event_hash(cls, v, values):
        if v is None:
            # Генерируем хэш из ключевых полей
            hash_data = {
                'event_name': values.get('event_name'),
                'event_datetime': values.get('event_datetime'),
                'user_agent': values.get('user_agent')
            }
            return hashlib.sha256(
                json.dumps(hash_data, sort_keys=True).encode()
            ).hexdigest()
        return v


class EventResponse(EventBase):
    id: int
    event_hash: str
    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat()}
    )
