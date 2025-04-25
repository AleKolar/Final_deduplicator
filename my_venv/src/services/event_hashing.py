import hashlib
import secrets
from typing import Dict, Any
import orjson

from my_venv.src.services.normalize import normalize_for_hashing


class HashGenerationError(Exception):
    pass


class EventHashService:
    @staticmethod
    def generate_unique_fingerprint(
            raw_data: Dict[str, Any],
            event_name: str
    ) -> str:
        try:
            # Фильтрация и нормализация полей
            processed_data = {
                k: normalize_for_hashing(v, k)
                for k, v in raw_data.items()
                if v is not None
            }

            # Дополнительная обработка для стабильности
            sorted_items = sorted(
                processed_data.items(),
                key=lambda x: (x[0], str(x[1])))

            fingerprint_payload = {
                "event": event_name,
                "fields": {
                    k: v for k, v in sorted_items
                }
            }

            serialized = orjson.dumps(
                fingerprint_payload,
                option=orjson.OPT_SORT_KEYS |
                       orjson.OPT_NON_STR_KEYS |
                       orjson.OPT_INDENT_2
            )

            return hashlib.sha3_256(serialized).hexdigest()

        except Exception as e:
            raise HashGenerationError(f"Fingerprint error: {str(e)}") from e

    @staticmethod
    def verify_fingerprint(
            event_hash: str,
            raw_data: Dict[str, Any],
            event_name: str
    ) -> bool:
        generated = EventHashService.generate_unique_fingerprint(
            raw_data,
            event_name
        )
        return secrets.compare_digest(event_hash, generated)