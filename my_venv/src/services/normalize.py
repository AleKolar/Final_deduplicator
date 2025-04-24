import re
from datetime import datetime
import json
from typing import Any, Dict
from my_venv.src.utils.cleaners import deep_clean
from my_venv.src.utils.datetime_utils import parse_datetime


def fix_invalid_json(data: str) -> Any:
    """Исправление распространённых ошибок в JSON"""
    try:
        # Попытка прямого парсинга
        return json.loads(data)
    except json.JSONDecodeError:
        # Исправление распространённых проблем
        fixed = re.sub(r"'\s*:\s*'", '": "', data)  # Ключи с одинарными кавычками
        fixed = re.sub(r"'\s*,\s*'", '", "', fixed)  # Элементы списка
        fixed = fixed.replace("'", '"')  # Все оставшиеся одинарные кавычки
        return json.loads(fixed)

def preprocess_input(data: dict) -> dict:
    """Предобработка входящих данных"""
    processed = {}
    for key, value in data.items():
        if isinstance(value, str):
            # Обработка строк, которые могут быть JSON
            if value.startswith(('[', '{')) and value.endswith((']', '}')):
                try:
                    processed[key] = fix_invalid_json(value)
                    continue
                except json.JSONDecodeError:
                    pass
        processed[key] = value
    return processed

def normalize_for_hashing(raw_data: Any, field_name: str) -> Dict[str, Any]:
    """Универсальная нормализация данных с обработкой времени"""

    def _convert_value(value: Any) -> str:
        """Рекурсивная конвертация значений"""
        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, str):
            try:
                return parse_datetime(value).isoformat()
            except ValueError:
                pass

        if isinstance(value, (dict, list)):
            return json.dumps(
                deep_clean(value),
                sort_keys=True,
                ensure_ascii=False,
                default=str
            )

        return str(value)

    # Глубокая очистка данных
    cleaned_data = deep_clean(raw_data)

    # Проверка на валидность данных после очистки
    if not cleaned_data or (
            isinstance(cleaned_data, (dict, list))
            and not cleaned_data
    ):
        return {}

    # Нормализация структуры
    normalized = {}
    if isinstance(cleaned_data, dict):
        for key in sorted(cleaned_data.keys()):
            converted = _convert_value(cleaned_data[key])
            if converted:
                normalized[key] = converted
    else:
        converted = _convert_value(cleaned_data)
        if converted:
            normalized[field_name] = converted

    return normalized


def process_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Полный цикл обработки данных"""
    cleaned = deep_clean(raw_data) or {}
    return {
        k: normalize_for_hashing(v, k)
        for k, v in cleaned.items()
        if v is not None and normalize_for_hashing(v, k)
    }