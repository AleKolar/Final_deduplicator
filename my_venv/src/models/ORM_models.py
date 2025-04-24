from sqlalchemy import Column, Integer, String, DateTime, func, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

from ..models.pydentic_models import EventBase


class Base(AsyncAttrs, DeclarativeBase):
    pass

class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    event_hash = Column(String(64), unique=True, nullable=False, index=True)
    raw_data = Column(JSONB, nullable=False)
    timestamps = Column(JSONB, nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    event_type = Column(String)

    __table_args__ = (
        Index('ix_raw_data_gin', 'raw_data', postgresql_using='gin'),
        Index('ix_timestamps_gin', 'timestamps', postgresql_using='gin'),
        Index('ix_event_hash', 'event_hash', postgresql_using='hash')
    )

    def update_from_pydantic(self, event: EventBase):
        self.raw_data = event.raw_data
        self.timestamps = event.timestamps
        self.event_hash = event.event_hash