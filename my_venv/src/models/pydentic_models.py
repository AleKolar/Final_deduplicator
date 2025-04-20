# pydantic_models.py
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, ValidationInfo, ConfigDict

from my_venv.src.services.event_hashing import generate_event_hash
from my_venv.src.services.normalize import normalize_for_hashing


class EventBase(BaseModel):
    event_hash: str
    event_name: str
    event_datetime: datetime
    profile_id: Optional[str] = None
    device_ip: Optional[str] = None
    raw_data: Dict[str, Any]
    created_at: datetime

    @classmethod
    def validate_event_hash(cls, v: str, values: ValidationInfo) -> str:
        # Извлекаем необходимые данные из модели
        raw_data = values.data.get('raw_data', {})
        event_name = values.data.get('event_name', '')

        # Нормализуем данные для хэширования
        normalized = normalize_for_hashing(raw_data, event_name)

        # Генерируем эталонный хэш
        expected_hash = generate_event_hash(normalized)

        if v != expected_hash:
            raise ValueError(
                f"Event hash mismatch. Expected: {expected_hash[:12]}..., got: {v[:12]}..."
            )
        return v


class EventCreate(EventBase):
    # Пред.подготовка, при необходимости др.валидаторы
    @classmethod
    def validate_event_name_length(cls, v: str) -> str:
        if len(v) > 100:
            raise ValueError("Event name exceeds maximum length (100 characters)")
        return v


class EventResponse(EventBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


