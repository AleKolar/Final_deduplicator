from pydantic import BaseModel, model_validator, ConfigDict, Field
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic_core import PydanticCustomError

from my_venv.src.services.event_hashing import EventHashService, HashGenerationError

# !!! Делаем возможным работу кода при наличии любых полей(избавляемся от обязательных полей)
class EventCreate(BaseModel):
    model_config = ConfigDict(
        extra='allow',
        validate_assignment=True,
        arbitrary_types_allowed=False
    )

    event_name: Optional[str] = Field(default=None)  # Теперь опционально
    event_datetime: Optional[datetime] = None  # Теперь опционально
    event_hash: Optional[str] = Field(
        None,
        min_length=64,
        max_length=64,
        pattern=r'^[a-f0-9]{64}$'
    )

    @model_validator(mode='after')
    def generate_event_hash(cls, instance: 'EventCreate') -> 'EventCreate':
        if instance.event_hash:
            return instance

        if not instance.event_name:
            # !!! Вот: случай, когда event_name отсутствует.
            return instance

        try:
            # Смотрим только на существующие поля
            present_fields = instance.model_dump(
                exclude_unset=True,
                exclude_none=True,
                exclude={'event_hash'}
            )

            instance.event_hash = EventHashService.generate_unique_fingerprint(
                present_fields,
                instance.event_name
            )
            return instance

        except HashGenerationError as e:
            raise PydanticCustomError(
                "hash_generation",
                "Hash generation failed: {reason}",
                {"reason": str(e)}
            )


class EventResponse(EventCreate):
    id: Optional[int] = None
    created_at: Optional[datetime] = None  # Делаем опционально
    raw_data: Dict[str, Any] = {}  # По умолчанию пустой словарь
    profile_id: Optional[str] = None
    device_ip: Optional[str] = None

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat()},
        populate_by_name=True
    )