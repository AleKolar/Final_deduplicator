from sqlalchemy import Column, Integer, String, DateTime, func, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = 'events'

    # Технические поля
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    event_hash = Column(String(64), unique=True, nullable=False, index=True)

    # Данные события
    raw_data = Column(JSONB, nullable=False)
    timestamps = Column(JSONB, nullable=False)  # Все временные метки

    # Системные метки времени
    processed_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index(
            'ix_raw_data_gin',
            'raw_data',
            postgresql_using='gin',
            postgresql_ops={'raw_data': 'jsonb_path_ops'}
        ),
        Index(
            'ix_timestamps_gin',
            'timestamps',
            postgresql_using='gin',
            postgresql_ops={'timestamps': 'jsonb_path_ops'}
        ),
        Index('ix_created_at', 'created_at', postgresql_ops={'created_at': 'DESC'})
    )

    def to_response(self) -> dict:
        """Конвертация в формат для ответа API"""
        return {
            "id": self.id,
            "created_at": self.created_at,
            "raw_data": self.raw_data,
            "timestamps": self.timestamps,
            "system_timestamps": {
                "processed_at": self.processed_at,
                "created_at": self.created_at
            }
        }