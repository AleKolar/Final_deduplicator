from sqlalchemy import Column, String, DateTime, JSON, Integer, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base
import asyncio

""" Создание таблицы events в БД PostgreSQL, так как таблица не создавалась, 
позже можно было отмониторить с использованием DBeaver """

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/events_db_1"

Base = declarative_base()

class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_hash = Column(String(64), unique=True, nullable=False)
    event_name = Column(String(100), index=True, nullable=True)  # Делаем опциональным
    event_datetime = Column(DateTime(timezone=True), nullable=True)  # Делаем опциональным
    profile_id = Column(String(50), nullable=True)  # Делаем опциональным
    device_ip = Column(String(15), nullable=True)  # Делаем опциональным
    raw_data = Column(JSON, nullable=True)  # Делаем опциональным
    created_at = Column(DateTime(timezone=True), server_default=func.now())

async def create_tables():
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Таблицы созданы!")


if __name__ == "__main__":
    asyncio.run(create_tables())

