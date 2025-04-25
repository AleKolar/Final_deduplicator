import json
import re
from datetime import datetime
from typing import Any, Dict, Optional, List

from my_venv.src.utils.data_cleaners import parse_datetime, deep_clean
from my_venv.src.utils.logger import logger


def normalize_for_hashing(raw_data: Any, field_name: str) -> Dict[str, Any]:
    """Универсальная нормализация данных для хеширования"""

    def _convert_value(value: Any) -> str:
        """Рекурсивная конвертация значений с улучшенной обработкой ошибок"""
        try:
            # Глубокая очистка перед обработкой
            cleaned_value = deep_clean(value)

            # Обработка дат
            if isinstance(cleaned_value, (datetime, str)):
                return parse_datetime(cleaned_value).isoformat()

            # Обработка вложенных структур
            if isinstance(cleaned_value, (dict, list)):
                return json.dumps(
                    cleaned_value,
                    sort_keys=True,
                    ensure_ascii=False,
                    default=str,
                    separators=(',', ':')
                )

            # Конвертация базовых типов
            return str(cleaned_value)

        except Exception as e:
            logger.warning(f"Normalization error for {field_name}: {str(e)}")
            return ""

    try:
        cleaned_data = deep_clean(raw_data)

        if isinstance(cleaned_data, dict):
            return {
                k: _convert_value(v)
                for k, v in cleaned_data.items()
                if v is not None
            }

        return {field_name: _convert_value(cleaned_data)}

    except Exception as e:
        logger.error(f"Critical normalization error: {str(e)}")
        return {}


def robust_experiments_parser(experiments_str: str) -> List[Dict[str, Any]]:
    """Устойчивый парсер экспериментов с обработкой некорректных данных"""
    experiments = []

    try:
        # Удаление лишних символов
        clean_str = re.sub(r"['\"\[\]\s]", "", experiments_str)
        items = [item.strip() for item in clean_str.split(",") if item.strip()]

        for item in items:
            if ":" not in item:
                continue

            key, value = item.split(":", 1)
            key = key.strip()
            value = value.strip().lower()

            # Определение типа значения
            if value == "true":
                parsed_value = True
            elif value == "false":
                parsed_value = False
            elif value == "null":
                parsed_value = None
            elif value.isdigit():
                parsed_value = int(value)
            elif re.match(r"^-?\d+\.?\d*$", value):
                parsed_value = float(value)
            else:
                parsed_value = value

            experiments.append({key: parsed_value})

    except Exception as e:
        logger.error(f"Experiment parser error: {str(e)}")

    return experiments


def fix_invalid_json(data: str) -> Optional[dict]:
    """Улучшенный корректор JSON с обработкой сложных случаев"""

    def _apply_fixes(s: str) -> str:
        """Последовательность исправлений с защитой от ошибок"""
        fixes = [
            (r"(?<!\\)'", '"'),  # Кавычки
            (r',\s*(?=\s*[}\]])', ''),  # Trailing commas
            (r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3'),  # Ключи
            (r':\s*([^"{\[][^,}\]\n]*)', r':"\1"')  # Значения
        ]

        for pattern, replacement in fixes:
            try:
                s = re.sub(pattern, replacement, s, flags=re.MULTILINE)
            except Exception as e:
                logger.warning(f"Regex error: {str(e)}")
        return s

    for attempt in range(3):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            data = _apply_fixes(data)

    logger.error("Failed to fix JSON after 3 attempts")
    return None


def preprocess_input(data: dict) -> dict:
    """Обработка входных данных с защитой от ошибок"""

    def _safe_parse(value: Any) -> Any:
        """Безопасная конвертация JSON строк"""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    processed = {}
    for key, value in data.items():
        try:
            if key == "experiments":
                processed[key] = robust_experiments_parser(str(value))
            else:
                processed[key] = _safe_parse(value)
        except Exception as e:
            logger.error(f"Field {key} processing error: {str(e)}")
            processed[key] = value

    return remove_empty_values(processed)


def remove_empty_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Рекурсивное удаление пустых значений с улучшенной логикой"""

    def _is_empty(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, (str, list, dict, bytes, tuple, set)):
            return len(val) == 0
        return False

    def _process(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _process(v) for k, v in value.items() if not _is_empty(v)}
        if isinstance(value, list):
            return [_process(i) for i in value if not _is_empty(i)]
        return value

    return _process(data)