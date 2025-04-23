import hashlib
import secrets
from typing import Dict, Any, Optional
import orjson
from my_venv.src.services.normalize import normalize_for_hashing  # Оставляем для нормализации, если необходимо

# !!! Делаем возможным работу кода при наличии любых полей(избавляемся от обязательных полей)
class HashGenerationError(Exception):
    """Ошибка генерации хеша."""
    pass


class EventHashService:
    @staticmethod
    def generate_unique_fingerprint(
            raw_data: Dict[str, Any],
            event_name: Optional[str] = None  # Это поле теперь опционально
    ) -> str:
        """
        Генерация отпечатка на основе сырых данных и имени события
        с учетом только присутствующих полей.
        Если event_name не предоставлено, будет использовано значение по умолчанию.
        """
        try:
            # Если event_name отсутствует, установим значение по умолчанию
            if event_name is None:
                event_name = "default_event"  # По умолчанию

            # Фильтрация и нормализация полей
            processed_data = {
                k: normalize_for_hashing(v, k)  # Вызываем нормализацию
                for k, v in raw_data.items()
                if v is not None  # Исключаем None значения
            }

            # Сортировка ключей для стабильности
            sorted_items = sorted(processed_data.items())

            # Формирование тела для отпечатка
            fingerprint_payload = {
                "event": event_name,
                "fields": dict(sorted_items)  # Включаем только обработанные поля
            }

            # Детерминированная сериализация
            serialized = orjson.dumps(
                fingerprint_payload,
                option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS
            )

            # Генерация хеша
            return hashlib.sha3_256(serialized).hexdigest()

        except Exception as e:
            raise HashGenerationError(f"Fingerprint error: {str(e)}") from e

    @staticmethod
    def verify_fingerprint(
            event_hash: str,
            raw_data: Dict[str, Any],
            event_name: Optional[str] = None  # Это поле теперь опционально
    ) -> bool:
        generated = EventHashService.generate_unique_fingerprint(
            raw_data,
            event_name
        )
        return secrets.compare_digest(event_hash, generated)