import re
from typing import AsyncIterator
import aio_pika
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

from fastapi.exceptions import RequestValidationError
from redis.asyncio import Redis

from my_venv.src.core.state import AppState
from my_venv.src.database.database import engine
from my_venv.src.models.ORM_models import Model
from my_venv.src.routers import events
from my_venv.src.config import settings
from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import JSONRepairEngine, DataNormalizer


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
async def json_fix_middleware(request: Request, call_next):
    if request.headers.get("content-type") == "application/json":
        try:
            body = await request.body()
            # Исправление JSON
            corrected = re.sub(
                r',\s*(?=\s*[}\]])',
                '',
                body.decode(),
                flags=re.MULTILINE
            )
            request.state._body = corrected.encode()  # Модифицируем тело запроса
        except json.JSONDecodeError:
            return JSONResponse(
                status_code=400,
                content={"detail": "Invalid JSON format"}
            )
        except Exception as e:
            logger.error(f"JSON fix error: {str(e)}")

    response = await call_next(request)
    return response


# Существующий middleware
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

from fastapi import Request
from fastapi.responses import JSONResponse
import json

@app.middleware("http")
async def json_validation_middleware(request: Request, call_next):
    if request.method in ("POST", "PUT", "PATCH"):
        try:
            body = await request.body()
            if body:
                # Попытка исправить JSON перед обработкой
                fixed_body = JSONRepairEngine.fix_string(body.decode())
                request._body = fixed_body.encode()
                json_data = json.loads(fixed_body)  # Проверка валидности

                # Обработка значений JSON для парсинга дат
                json_data = DataNormalizer.deep_clean(json_data)

                # На этом этапе можно устанавливать измененные данные обратно в request
                request._json_data = json_data  # Пример, как можно хранить обработанные данные

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON received: {str(e)}")
            return JSONResponse(
                status_code=422,
                content={
                    "detail": [{
                        "type": "json_invalid",
                        "loc": ["body"],
                        "msg": "Invalid JSON format",
                        "ctx": {"error": str(e)}
                    }]
                }
            )
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            return JSONResponse(
                status_code=422,
                content={
                    "detail": [{
                        "type": "json_error",
                        "loc": ["body"],
                        "msg": "Error while processing the JSON body",
                        "ctx": {"error": str(e)}
                    }]
                }
            )

    return await call_next(request)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Обработчик ошибок валидации с логированием"""
    logger.warning(
        "Validation error for request %s %s: %s",
        request.method,
        request.url.path,
        exc.errors()
    )

    return JSONResponse(
        status_code=422,
        content={
            "detail": [
                {
                    "loc": list(error["loc"]),
                    "msg": error["msg"],
                    "type": error["type"]
                } for error in exc.errors()
            ]
        }
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