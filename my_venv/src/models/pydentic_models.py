from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, model_validator, ConfigDict

import logging

from ..services.event_hashing import EventHashService
from ..utils.cleaners import deep_clean
from ..utils.logger import logger
from ..utils.serializer import JSONSerializer



class EventBase(BaseModel):
    event_type: str
    details: Dict[str, Any]
    timestamps: Dict[str, datetime] = Field(
        default_factory=dict,
        description="Все обнаруженные временные метки в событии"
    )
    event_hash: str = Field(
        ...,
        description="Уникальный хеш события (генерируется автоматически)"
    )

    model_config = ConfigDict(
        extra='allow',
        validate_assignment=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
            bytes: lambda v: v.decode('utf-8', errors='replace')
        },
        arbitrary_types_allowed=True
    )

    @model_validator(mode='before')
    @classmethod
    def preprocess_and_hash(cls, data: Any) -> Dict[str, Any]:
        """Комплексная предобработка данных и генерация хеша"""
        try:
            # Очистка данных с конвертацией bytes
            cleaned = deep_clean(data)
            cleaned = cls._convert_bytes(cleaned)  # Добавлена конвертация bytes

            if not cleaned or not isinstance(cleaned, dict):
                raise ValueError("Событие не содержит данных для обработки")

            # Фикс JSON-строк
            for key, value in cleaned.items():
                if isinstance(value, str) and value.startswith(('{', '[')):
                    try:
                        cleaned[key] = JSONSerializer.fix_json_string(value)
                    except Exception as e:
                        logger.warning(f"Failed to parse {key}: {str(e)}")

            # Извлечение временных меток
            timestamps = cls._pre_extract_timestamps(cleaned)

            # Генерация хеша
            serialized = JSONSerializer.serialize_for_hashing({
                "data": cleaned,
                "timestamps": timestamps
            })
            event_hash = EventHashService.generate_unique_fingerprint(serialized)

            return {
                **cleaned,  # Исправлено: передача данных напрямую
                "timestamps": timestamps,
                "event_hash": event_hash
            }

        except Exception as e:
            logger.error(f"Ошибка предобработки: {str(e)}")
            logger.debug(f"Исходные данные: {data}")
            raise ValueError(f"Ошибка валидации данных: {str(e)}")

    @classmethod
    def _convert_bytes(cls, data: Any) -> Any:
        """Рекурсивная конвертация bytes в строки"""
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='replace')
        if isinstance(data, dict):
            return {k: cls._convert_bytes(v) for k, v in data.items()}
        if isinstance(data, list):
            return [cls._convert_bytes(item) for item in data]
        return data

    @classmethod
    def _pre_extract_timestamps(cls, data: dict) -> Dict[str, datetime]:
        """Извлечение временных меток из данных"""
        timestamp_fields = [
            'ts', 'dt_add', 'event_receive_dt_str',
            'timestamp', 'event_time', 'created_at',
            'event_receive_timestamp', 'time'
        ]
        timestamps = {}

        for field in timestamp_fields:
            if value := data.get(field):
                try:
                    timestamps[field] = datetime.fromisoformat(value)
                except (TypeError, ValueError):
                    continue
        return timestamps


class EventCreate(EventBase):
    raw_data: Dict[str, Any] = Field(
        ...,
        description="Все исходные данные события"
    )

    @model_validator(mode='after')
    def validate_hash_length(self) -> 'EventCreate':
        if len(self.event_hash) != 64:
            raise ValueError("Некорректный формат хеша (должен быть 64 символа)")
        return self

    @property
    def main_timestamp(self) -> Optional[datetime]:
        priority_order = ['ts', 'dt_add', 'event_time', 'timestamp']
        for field in priority_order:
            if field in self.timestamps:
                return self.timestamps[field]
        return None


class EventResponse(EventBase):
    id: Optional[int] = Field(
        None,
        description="Уникальный идентификатор записи в БД"
    )
    created_at: datetime = Field(
        ...,
        description="Дата сохранения в БД"
    )
    system_timestamps: Dict[str, datetime] = Field(
        default_factory=dict,
        description="Системные временные метки"
    )

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )