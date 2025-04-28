from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import Column, String, DateTime, JSON, Integer, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
import asyncio

""" Создание таблицы events в БД PostgreSQL, так как таблица не создавалась, 
позже можно было отмониторить с использованием DBeaver """

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/events_db_1"

Model = declarative_base()

class Event(Model):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    event_name: Mapped[str] = mapped_column(String(100), default="unknown")
    event_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    profile_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    device_ip: Mapped[Optional[str]] = mapped_column(String(15))
    raw_data: Mapped[Dict[str, Any]] = mapped_column(JSON, default={})
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

async def create_tables():
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.create_all)
    print("Таблицы созданы!")

if __name__ == "__main__":
    asyncio.run(create_tables())

