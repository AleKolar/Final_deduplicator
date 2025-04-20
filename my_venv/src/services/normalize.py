from typing import Dict, Any
""" Проверка хэша происходит на основе нормализованных данных,
в логике, что event_hash производное от нормализованных данных """


def normalize_for_hashing(raw_data: Dict[str, Any], event_name: str) -> Dict[str, Any]:
    """Нормализация только для генерации хэша без изменения исходных данных"""
    # 1. Базовые обязательные поля
    normalized = {
        "event_name": event_name,
        "client_id": raw_data.get("client_id"),
        "content_id": raw_data.get("content_id"),
        "product_id": raw_data.get("product_id")
    }

    # 2. Динамические поля из raw_data (исключая технические и пустые)
    additional_fields = {
        k: v for k, v in raw_data.items()
        if not k.startswith('_')
           and k not in normalized  # Исключаем уже обработанные
           and v not in (None, "", [], {})
    }

    # 3. Объединяем и сортируем
    combined = {**normalized, **additional_fields}
    return dict(sorted(combined.items()))


