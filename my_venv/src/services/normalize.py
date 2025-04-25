import re
from datetime import datetime
import json
from typing import Any, Dict

from my_venv.src.utils.data_cleaners import deep_clean, parse_datetime
from my_venv.src.utils.logger import logger


def fix_invalid_json(data: str) -> Any:
    """Исправление распространённых ошибок в JSON"""
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        # Попытка исправления ключей с одинарными кавычками и подобных ошибок
        fixed = re.sub(r"'\s*:\s*'", '": "', data)
        fixed = re.sub(r"'\s*,\s*'", '", "', fixed)
        fixed = fixed.replace("'", '"')
        try:
            return json.loads(fixed)
        except json.JSONDecodeError as e:
            # Логируем ошибку обработки, чтобы легче было отследить проблему
            logger.error(f"Ошибка преобразования JSON: {str(e)}")
            return {}

# Тут расширяем и расширяем возможности пред. обработки, главное не запутаться
def preprocess_input(data: dict) -> dict:
    """Предобработка входящих данных с попыткой исправления JSON."""
    processed = {}
    for key, value in data.items():
        if isinstance(value, str):
            if key == "experiments":
                # Если значение - строка с массивом, пытаемся его разобрать
                try:
                    # Пробуем преобразовать одинарные кавычки в двойные и разобрать строку
                    value = json.loads(fix_invalid_json(value))
                except json.JSONDecodeError:
                    logger.error(f"Ошибка при парсинге experiments: {value}")
                    value = []  # Подставляем пустой массив в случае ошибки
            else:
                # Пробуем исправить некорректный JSON
                try:
                    value = json.loads(fix_invalid_json(value))
                except json.JSONDecodeError:
                    logger.error(f"Ошибка при парсинге ключа '{key}': {value}")
                    value = value  # Оставляем значение неизменным
        elif isinstance(value, list):
            # Если значение — это список, обрабатываем каждый его элемент
            value = [str(item) for item in value]  # Приводим все элементы к строкам

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