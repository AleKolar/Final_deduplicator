from typing import Dict, Any
from collections import OrderedDict


def normalize_for_hashing(raw_data: Dict[str, Any], event_name: str) -> Dict[str, Any]:
    """Улучшенная нормализация данных для генерации хэша"""
    # 1. Базовые поля для всех событий
    base_fields = OrderedDict([
        ("event_name", event_name),
        ("client_id", raw_data.get("client_id")),
        ("content_id", raw_data.get("content_id")),
        ("profile_id", raw_data.get("profile_id")),
        ("device_fingerprint", raw_data.get("device_fingerprint"))
    ])

    # 2. Динамические поля из raw_data
    dynamic_fields = OrderedDict()
    for key in sorted(raw_data.keys()):
        if key not in base_fields and not key.startswith(('_', 'internal_')):
            value = raw_data.get(key)
            if value not in (None, "", [], {}):
                dynamic_fields[key] = value

    # 3. Объединяем и фильтруем
    normalized = OrderedDict()
    normalized.update(base_fields)
    normalized.update(dynamic_fields)

    # Удаление None значений
    return OrderedDict(
        (k, v) for k, v in normalized.items()
        if v is not None
    )
