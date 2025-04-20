import hashlib
from typing import Dict, Any
import orjson

from my_venv.src.services.normalize import normalize_for_hashing


def generate_event_hash(raw_data: Dict[str, Any], event_name: str) -> str:
    """Генерирует SHA-256 хэш от нормализованных данных."""
    normalized_data = normalize_for_hashing(raw_data, event_name)
    return hashlib.sha256(
        orjson.dumps(
            normalized_data,
            option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS
        )
    ).hexdigest()

def verify_event_hash(event_hash: str, raw_data: Dict, event_name: str) -> bool:
    """Проверяет, соответствует ли хэш текущим данным."""
    return event_hash == generate_event_hash(raw_data, event_name)