from fastapi import status
from typing import Optional, Dict, Any
import json

class BaseEventException(Exception):
    """Базовое исключение для событий"""
    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
            "status_code": self.status_code
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

class EventValidationError(BaseEventException):
    """Ошибка валидации данных события"""
    def __init__(
        self,
        message: str = "Invalid event data",
        details: Optional[Dict[str, Any]] = None,
        fields: Optional[Dict[str, str]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details={
                "fields": fields or {},
                **(details or {})
            }
        )

class DuplicateEventError(BaseEventException):
    """Ошибка дублирования события"""
    def __init__(
        self,
        message: str = "Event already exists",
        event_hash: Optional[str] = None,
        existing_event_id: Optional[int] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_409_CONFLICT,
            details={
                "event_hash": event_hash,
                "existing_event_id": existing_event_id
            }
        )

class DatabaseError(BaseEventException):
    """Ошибка работы с базой данных"""
    def __init__(
        self,
        message: str = "Database operation failed",
        sql_error: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={
                "sql_error": sql_error,
                "query_params": query_params or {}
            }
        )

class EventProcessingError(BaseEventException):
    """Ошибка обработки события"""
    def __init__(
        self,
        message: str = "Event processing failed",
        processing_stage: Optional[str] = None,
        raw_data: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            details={
                "processing_stage": processing_stage,
                "raw_data": raw_data or {}
            }
        )

class EventNotFoundError(BaseEventException):
    """Событие не найдено"""
    def __init__(
        self,
        message: str = "Event not found",
        event_id: Optional[int] = None,
        event_hash: Optional[str] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_404_NOT_FOUND,
            details={
                "event_id": event_id,
                "event_hash": event_hash
            }
        )

class ExternalServerError(BaseEventException):
    """Ошибка взаимодействия с внешним сервером"""
    def __init__(
        self,
        message: str = "External server request failed",
        url: Optional[str] = None,
        method: Optional[str] = None,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_502_BAD_GATEWAY,
            details={
                "external_url": url,
                "http_method": method,
                "external_status": status_code,
                "response_body": response_text
            }
        )

class MessageQueueError(BaseEventException):
    """Ошибка работы с очередями сообщений"""
    def __init__(
        self,
        message: str = "Message queue operation failed",
        queue_name: Optional[str] = None,
        error_details: Optional[str] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            details={
                "queue": queue_name,
                "error": error_details
            }
        )