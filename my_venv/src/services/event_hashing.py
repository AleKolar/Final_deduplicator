import hashlib
import secrets
from typing import Dict, Any, Optional
import orjson
from datetime import datetime, timezone
from my_venv.src.services.normalize import normalize_for_hashing


class HashGenerationError(Exception):
    pass


class EventHashService:
    @staticmethod
    def generate(
            raw_data: Dict[str, Any],
            event_name: str,
            salt: Optional[str] = None,
            use_timestamp: bool = False  # Новый параметр
    ) -> str:
        """
        Улучшенная генерация хэша:
        - Контроль над временными метками
        - Оптимизированная сериализация
        - Защита от коллизий
        """
        try:
            # Нормализация данных (ваш отличный нормалайзер)
            normalized_data = normalize_for_hashing(raw_data, event_name)

            # Базовый объект для хэширования
            hash_payload = {
                "event": event_name,
                "data": normalized_data
            }

            # Опциональные компоненты
            if salt:
                hash_payload["salt"] = salt
            if use_timestamp:
                hash_payload["timestamp"] = datetime.now(timezone.utc).isoformat()

            # Сериализация с сортировкой ключей
            serialized = orjson.dumps(
                hash_payload,
                option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS
            )

            return hashlib.sha3_256(serialized).hexdigest()

        except Exception as e:
            raise HashGenerationError(f"Hash generation failed: {str(e)}") from e

    @staticmethod
    def verify(
            event_hash: str,
            raw_data: Dict[str, Any],
            event_name: str,
            salt: Optional[str] = None,
            use_timestamp: bool = False
    ) -> bool:
        """Улучшенная верификация с защитой от timing-атак"""
        try:
            generated_hash = EventHashService.generate(
                raw_data,
                event_name,
                salt,
                use_timestamp
            )
            return secrets.compare_digest(event_hash, generated_hash)
        except Exception:
            return False


# Для обратной совместимости
def generate_event_hash(raw_data: Dict[str, Any], event_name: str) -> str:
    return EventHashService.generate(raw_data, event_name, use_timestamp=False)


def verify_event_hash(event_hash: str, raw_data: Dict[str, Any], event_name: str) -> bool:
    return EventHashService.verify(event_hash, raw_data, event_name)

