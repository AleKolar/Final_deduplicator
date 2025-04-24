from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, model_validator, ConfigDict

from ..create_tables import Event
from ..utils.data_cleaners import deep_clean, parse_datetime
from ..utils.logger import logger
from ..utils.serializer import JSONSerializer
from ..services.event_hashing import EventHashService


# class EventBase(BaseModel):
#     event_type: Optional[str] = Field(
#         default=None,
#         description="Автоматически определяемый тип события"
#     )
#     raw_data: Dict[str, Any] = Field(
#         default_factory=dict,
#         description="Исходные необработанные данные"
#     )
#     timestamps: Dict[str, datetime] = Field(
#         default_factory=dict,
#         description="Извлеченные временные метки"
#     )
#     event_hash: str = Field(
#         default="",
#         description="Автоматически генерируемый хеш"
#     )
#
#     model_config = ConfigDict(
#         extra='allow',
#         validate_assignment=True,
#         json_encoders={
#             datetime: lambda v: v.isoformat(),
#             bytes: lambda v: v.decode('utf-8', errors='replace')
#         },
#         arbitrary_types_allowed=True
#     )

class EventBase(BaseModel):
    event_type: Optional[str] = Field(
        default=None,
        description="Автоматически определяемый тип события"
    )
    raw_data: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Исходные необработанные данные"
    )
    timestamps: Optional[Dict[str, datetime]] = Field(
        default_factory=dict,
        description="Извлеченные временные метки"
    )
    event_hash: Optional[str] = Field(
        default=None,
        description="Автоматически генерируемый хеш"
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
        try:
            if not isinstance(data, dict):
                raise ValueError("Данные должны быть словарем")

            # Сохраняем исходные данные
            raw_data = data.copy()

            # Очистка и конвертация
            cleaned = deep_clean(data)
            cleaned = cls.convert_bytes_recursive(cleaned)

            # Извлечение временных меток
            timestamps = cls._pre_extract_timestamps(cleaned)

            # Генерация хеша
            serialized = JSONSerializer.serialize_for_hashing({
                "raw_data": raw_data,
                "timestamps": timestamps
            })
            event_hash = EventHashService.generate_unique_fingerprint(serialized)

            return {
                "raw_data": raw_data,
                "timestamps": timestamps,
                "event_hash": event_hash,
                "event_type": cls._detect_event_type(cleaned)
            }

        except Exception as e:
            logger.error(f"Ошибка предобработки: {str(e)}", exc_info=True)
            raise ValueError(f"Ошибка валидации данных: {str(e)}")

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
                    # Используем parse_datetime из data_cleaners
                    timestamps[field] = parse_datetime(value)
                except ValueError as e:
                    logger.warning(f"Невалидная временная метка в поле {field}: {str(e)}")
        return timestamps

    @classmethod
    def _detect_event_type(cls, data: dict) -> Optional[str]:
        """Логика определения типа события"""
        type_mapping = {
            'purchase': ['order_id', 'amount'],
            'pageview': ['url', 'page_view'],
            'login': ['username', 'password']
        }

        for event_type, fields in type_mapping.items():
            if all(field in data for field in fields):
                return event_type
        return None

    @staticmethod
    def convert_bytes_recursive(data: Any) -> Any:
        """Рекурсивная конвертация bytes в строки"""
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='replace')
        if isinstance(data, dict):
            return {k: EventBase.convert_bytes_recursive(v) for k, v in data.items()}
        if isinstance(data, list):
            return [EventBase.convert_bytes_recursive(item) for item in data]
        return data


class EventCreate(EventBase):
    @model_validator(mode='after')
    def validate_required_fields(self) -> 'EventCreate':
        if len(self.event_hash) != 64:
            raise ValueError("Некорректная длина хеша")
        return self


class EventResponse(EventBase):
    id: Optional[int] = None
    #created_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.now, description="Дата сохранения в БД")
    processed_at: Optional[datetime] = None

    @classmethod
    def from_orm(cls, db_event: 'Event'):
        return cls(
            id=db_event.id,
            created_at=db_event.created_at,
            processed_at=db_event.processed_at,
            raw_data=db_event.raw_data,
            timestamps=db_event.timestamps,
            event_hash=db_event.event_hash,
            event_type=db_event.raw_data.get('event_type')
        )

class EventCreateResponse(EventResponse):
    unique_count: int = Field(..., description="Количество уникальных событий")
    duplicate_count: int = Field(..., description="Количество дубликатов")

