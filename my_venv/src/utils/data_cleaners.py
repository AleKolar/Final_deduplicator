from typing import Any, Union
import json
import re
from datetime import datetime
from decimal import Decimal

from my_venv.src.utils.logger import logger

__all__ = ['deep_clean', 'parse_datetime']

DATE_FORMATS = [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
    '%Y%m%dT%H%M%S',
    '%d.%m.%Y %H:%M:%S',
    '%m/%d/%Y %I:%M %p'
]


def parse_datetime(value: Union[str, datetime]) -> datetime:
    """Универсальный парсер дат с обработкой исключений"""
    if isinstance(value, datetime):
        return value

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)

    if isinstance(value, datetime):
        return value

    value = str(value).strip()

    # Попытка ISO формата
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass

    # Кастомные форматы
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Поиск даты в строке с мусором
    match = re.search(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}', value)
    if match:
        return parse_datetime(match.group())

    raise ValueError(f"Нераспознанный формат даты: {value}")


def deep_clean(data: Any) -> Any:
    """Рекурсивная очистка и нормализация данных"""

    def _is_meaningful(value: Any) -> bool:
        """Проверка значимости значения"""
        if isinstance(value, (str, bytes)):
            return bool(value.strip())
        return value is not None

    def _fix_json_string(json_str: str) -> str:
        """Исправление ошибок в JSON строках"""
        fixes = [
            (r"(?<!\\)'", '"'),
            (r"\\x([0-9a-fA-F]{2})", lambda m: chr(int(m.group(1), 16))),
            (r",\s*?([}\]])", r"\1")
        ]
        for pattern, replacement in fixes:
            json_str = re.sub(pattern, replacement, json_str)
        return json_str

    def _process_value(value: Any) -> Any:
        """Рекурсивная обработка значения"""
        try:
            if isinstance(value, (int, float)):
                try:
                    return datetime.fromtimestamp(value)
                except ValueError:
                    return value
            # Обработка коллекций
            if isinstance(value, dict):
                return {
                    k: _process_value(v)
                    for k, v in value.items()
                    if not k.startswith(('__', 'javascript', 'xml'))
                       and _is_meaningful(v)
                }

            if isinstance(value, (list, tuple, set)):
                return [_process_value(item) for item in value
                        if _is_meaningful(item)]

            # Конвертация специальных типов
            if isinstance(value, bytes):
                return value.decode('utf-8', errors='replace')

            if isinstance(value, Decimal):
                return float(value)

            if isinstance(value, datetime):
                return value.isoformat()

            # Обработка строк
            if isinstance(value, str):
                value = value.strip()

                # Попытка парсинга JSON
                if value and value[0] in ('{', '[') and value[-1] in ('}', ']'):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        try:
                            return json.loads(_fix_json_string(value))
                        except json.JSONDecodeError:
                            pass

                # Парсинг даты
                try:
                    return parse_datetime(value).isoformat()
                except ValueError:
                    pass

            return value

        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return None

    try:
        # Основная обработка
        cleaned = _process_value(data)

        # Финальная валидация
        json.dumps(
            cleaned,
            default=lambda o: str(o) if not isinstance(o, (str, int, float, bool)) else o
        )
        return cleaned

    except Exception as e:
        logger.error(f"Deep clean failed: {str(e)}", exc_info=True)
        return {"__error__": str(e)}