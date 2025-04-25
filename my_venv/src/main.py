from typing import AsyncIterator
import aio_pika
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from redis.asyncio import Redis
from starlette.responses import JSONResponse

from my_venv.src.core.state import AppState
from my_venv.src.database.database import engine
from my_venv.src.models.ORM_models import Model
from my_venv.src.routers import events
from my_venv.src.config import settings
from my_venv.src.utils.logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[AppState]:
    # Инициализация базы данных
    logger.info("Creating database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.create_all)

    # Инициализация Redis
    logger.info("Connecting to Redis...")
    redis = Redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        auto_close_connection_pool=True
    )

    # Подключение к RabbitMQ
    logger.info("Connecting to RabbitMQ...")
    rabbit_connection = await aio_pika.connect(settings.RABBITMQ_URL)
    # rabbit_channel = await rabbit_connection.channel()

    try:
        logger.info("Application startup complete")
        yield {
            "redis": redis,
            "rabbit_connection": rabbit_connection,
            "db_engine": engine
        }
    finally:
        logger.info("Closing connections...")
        await redis.close()
        await rabbit_connection.close()
        await engine.dispose()
        logger.info("Connections closed")


app = FastAPI(lifespan=lifespan)
app.include_router(events.router)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"message": "An error occurred", "details": str(e)}
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "my_venv.src.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        reload_dirs=['C:\\Users\\User\\pythonProject\\Project_dedup_19.04.25\\my_venv\\src'],
        reload_includes=["*.py"]
    )

# uvicorn my_venv.src.main:app --host 127.0.0.1 --port 8001 --log-level debug