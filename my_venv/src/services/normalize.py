from typing import Dict, Any
from collections import OrderedDict as ColOrderedDict


def normalize_for_hashing(raw_data: Dict[str, Any], event_name: str) -> Dict[str, Any]:
    """Улучшенная нормализация для генерации хэша"""
    # 1. Создаем базовую структуру с явными типами
    normalized: ColOrderedDict[str, Any] = ColOrderedDict([
        ("event_name", event_name),
        ("client_id", None),
        ("content_id", None),
        ("product_id", None)
    ])

    # 2. Заполняем базовые поля из raw_data
    for key in normalized.keys():
        if key != "event_name" and raw_data.get(key) not in (None, "", [], {}):
            normalized[key] = raw_data.get(key)

    # 3. Добавляем динамические поля
    for key in sorted(raw_data.keys()):
        if (
                key not in normalized
                and not key.startswith(('_', 'event_'))
                and raw_data[key] not in (None, "", [], {})
        ):
            normalized[key] = raw_data[key]

    # 4. Фильтруем и возвращаем обычный dict (сохраняя порядок)
    return dict(
        (k, v)
        for k, v in normalized.items()
        if v is not None
    )

