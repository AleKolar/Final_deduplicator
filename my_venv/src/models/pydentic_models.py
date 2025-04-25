import json
from pydantic import BaseModel, model_validator, ConfigDict, Field, field_validator
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pydantic_core import PydanticCustomError

from my_venv.src.services.event_hashing import EventHashService, HashGenerationError
from my_venv.src.services.normalize import fix_invalid_json
from my_venv.src.utils.logger import logger


class EventCreate(BaseModel):
    id: Optional[int] = Field(None)
    event_name: Optional[str] = Field(None)
    event_datetime: Optional[datetime] = Field(None)
    event_hash: Optional[str] = Field(
        None,
        pattern=r'^[a-f0-9]{64}$'
    )
    profile_id: Optional[str] = Field(None)
    device_ip: Optional[str] = Field(None)
    created_at: Optional[datetime] = Field(None)
    raw_data: Optional[dict] = Field(default_factory=dict)

    @model_validator(mode="before")
    def parse_string_fields(cls, values: dict) -> dict:
        for field in ["experiments", "discount_items_ids", "discount"]:
            field_value = values.get(field)
            if isinstance(field_value, str):
                try:
                    # Используем fix_invalid_json для коррекции
                    values[field] = fix_invalid_json(field_value) or []
                except Exception as e:
                    logger.error(f"Error parsing {field}: {str(e)}")
                    values[field] = []
        return values

    @field_validator("id", mode="before")
    def validate_id(cls, v):
        return str(v) if v else None

    model_config = {
        "extra": "allow",
        "arbitrary_types_allowed": True
    }

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

class EventRequest(BaseModel):
    feedback_text: str = ""
    experiments: list = Field(default_factory=list)
    client_id: Optional[str] = Field(None)
    playtime_ms: Optional[Union[int, float]] = Field(
        None,
        ge=0,
        description="Playtime in milliseconds (optional)"
    )
    duration: Optional[Union[int, float]] = Field(None)

    @field_validator('playtime_ms', mode="before")
    def validate_playtime(cls, v):
        if v in (None, "", "null"):
            return None
        if isinstance(v, str):
            try:
                return float(v) if '.' in v else int(v)
            except ValueError:
                raise ValueError("playtime_ms must be numeric")
        return v