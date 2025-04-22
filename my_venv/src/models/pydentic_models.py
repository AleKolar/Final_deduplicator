# from datetime import datetime
# from pydantic import BaseModel, field_validator, model_validator
# from typing import Optional, Dict, Any
#
# class EventBase(BaseModel):
#     id: Optional[int] = None
#     event_hash: str
#     event_name: str
#     event_datetime: datetime
#     created_at: Optional[datetime] = None
#     raw_data: Dict[str, Any]
#
#     class Config:
#         json_encoders = {
#             datetime: lambda v: v.isoformat()
#         }
#
# class EventCreate(EventBase):
#     @model_validator(mode='before')
#     @classmethod
#     def validate_required_fields(cls, values):
#         required = {'event_name', 'event_datetime'}
#         if not required.issubset(values.keys()):
#             missing = required - set(values.keys())
#             raise ValueError(f"Missing required fields: {missing}")
#         return values
#
#     @field_validator('*', mode='before')
#     @classmethod
#     def validate_empty_values(cls, value, field):
#         if field.name in ['event_name', 'event_datetime']:
#             if not value:
#                 raise ValueError(f"{field.name} cannot be empty")
#         return value
#
# class EventResponse(EventBase):
#     class Config:
#         from_attributes = True

from pydantic import BaseModel, ValidationError, field_validator, model_validator
from datetime import datetime
from typing import Optional, Dict, Any


class EventBase(BaseModel):
    id: Optional[int] = None
    event_hash: str
    event_name: str
    event_datetime: datetime
    created_at: Optional[datetime] = None
    raw_data: Dict[str, Any]

    # Валидация конкретных полей
    @classmethod
    @field_validator('event_name', 'event_datetime', mode='before')
    def validate_required_fields(cls, value):
        """Проверка обязательных полей перед валидацией"""
        if not value:
            raise ValueError("Field cannot be empty")
        return value

    # Валидация всей модели
    @model_validator(mode='after')
    def validate_event_structure(self):
        """Комплексная проверка после валидации"""
        if not self.event_name.strip():
            raise ValueError("Event name must not be empty")

        if self.event_datetime < datetime.now():
            raise ValueError("Event datetime must be in the future")

        return self


class EventCreate(EventBase):
    # Валидация с преобразованием данных
    @classmethod
    @field_validator('event_datetime', mode='before')
    def parse_datetime(cls, value):
        """Преобразование строки в datetime"""
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value


class EventResponse(EventBase):
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }