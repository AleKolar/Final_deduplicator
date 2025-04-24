from sqlalchemy import Column, String, DateTime, Integer, func, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import create_async_engine, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
import asyncio

""" Создание таблицы events в БД PostgreSQL, так как таблица не создавалась, 
позже можно было отмониторить с использованием DBeaver """


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/events_db_1"


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

    event_type = Column(String)

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


# async def create_tables():
#     engine = create_async_engine(DATABASE_URL)
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)
#     print("Таблицы успешно созданы!")

async def recreate_tables():
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("Таблицы успешно пересозданы!")


if __name__ == "__main__":
    asyncio.run(recreate_tables())