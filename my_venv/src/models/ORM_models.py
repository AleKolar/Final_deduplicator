from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import String, DateTime, JSON, Index, func, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Model(DeclarativeBase):
    pass

class EventIncomingORM(Model):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_hash: Mapped[str] = mapped_column(String(64), unique=True)
    event_name: Mapped[str] = mapped_column(String(100), default="unknown")
    event_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    profile_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    device_ip: Mapped[Optional[str]] = mapped_column(String(15))
    raw_data: Mapped[Dict[str, Any]] = mapped_column(JSON, default={})
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True) , server_default=func.now(), index=True)

    def __init__(self, **kwargs):
        system_fields = {'event_hash', 'event_name'}
        sys_data = {k: v for k, v in kwargs.items() if k in system_fields}
        raw_data = {k: v for k, v in kwargs.items() if k not in system_fields}
        super().__init__(**sys_data, raw_data=raw_data)

    __table_args__ = (
        Index("idx_main_analytics", "event_name", "event_datetime", "profile_id", "created_at"),
    ) # для аналитики, предположительно, запросы будут по этим полям

    def to_response(self) -> dict: # !!! А это: сериализация исходящих данных
        return {
            "id": self.id,
            "event_hash": self.event_hash,
            "event_name": self.event_name,
            "event_datetime": self.event_datetime,
            "profile_id": self.profile_id,
            "device_ip": self.device_ip,
            "raw_data": self.raw_data,
            "created_at": self.created_at
        }