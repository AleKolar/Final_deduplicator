from pydantic import BaseModel, model_validator, ConfigDict, Field
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic_core import PydanticCustomError

from my_venv.src.services.event_hashing import EventHashService, HashGenerationError


class EventCreate(BaseModel):
    model_config = ConfigDict(
        extra='allow',
        validate_assignment=True,
        arbitrary_types_allowed=False
    )

    event_name: str = Field(..., min_length=1)
    event_datetime: datetime
    event_hash: Optional[str] = Field(
        None,
        min_length=64,
        max_length=64,
        pattern=r'^[a-f0-9]{64}$'
    )

    @model_validator(mode='after')
    def generate_event_hash(self) -> 'EventCreate':
        if self.event_hash:
            return self

        try:
            # Используем только существующие поля
            present_fields = self.model_dump(
                exclude_unset=True,
                exclude_none=True,
                exclude={'event_hash'}
            )

            self.event_hash = EventHashService.generate_unique_fingerprint(
                present_fields,
                self.event_name
            )
            return self

        except HashGenerationError as e:
            raise PydanticCustomError(
                "hash_generation",
                "Hash generation failed: {reason}",
                {"reason": str(e)}
            )


class EventResponse(EventCreate):
    id: Optional[int] = None
    created_at: datetime
    raw_data: Dict[str, Any]
    profile_id: Optional[str] = None  # Добавляем новые поля
    device_ip: Optional[str] = None

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat()},
        populate_by_name=True
    )