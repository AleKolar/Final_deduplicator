import json
import re
from datetime import datetime
from typing import Any, Dict, Optional, List

from my_venv.src.utils.data_cleaners import parse_datetime, deep_clean
from my_venv.src.utils.logger import logger


def normalize_for_hashing(raw_data: Any, field_name: str) -> Dict[str, Any]:
    """Нормализация данных с полным игнорированием null"""
    cleaned_data = deep_clean(raw_data)

    def _convert(value: Any) -> str:
        if value in (None, "", "null", "none"):
            return ""
        if isinstance(value, (datetime, str)):
            return parse_datetime(value).isoformat()
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, default=str)
        return str(value)

    if isinstance(cleaned_data, dict):
        return {
            k: _convert(v)
            for k, v in cleaned_data.items()
            if v is not None
        }
    return {field_name: _convert(cleaned_data)} if cleaned_data is not None else {}


def robust_experiments_parser(experiments_str: str) -> List[Dict[str, Any]]:
    """Парсер экспериментов с улучшенной обработкой чисел"""
    experiments = []
    try:
        items = re.findall(r"'?(.*?:.*?)'?(?:,|$)", experiments_str)
        for item in items:
            if ":" not in item:
                continue

            key, value = item.split(":", 1)
            key = key.strip()
            value = value.strip()

            # Автоматическое определение типа
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = value.lower() if value.lower() in ('true', 'false', 'null') else value

            experiments.append({key: parsed})

    except Exception as e:
        logger.error(f"Experiment parsing failed: {str(e)}")

    return experiments


def fix_invalid_json(data: str) -> Optional[dict]:
    """Исправление JSON с обработкой сложных случаев"""
    def _apply_fixes(s: str) -> str:
        s = re.sub(r',\s*(?=\s*[}\]])', '', s, flags=re.MULTILINE)
        s = re.sub(r"(?<!\\)'", '"', s)
        s = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', s)
        s = re.sub(r':\s*(true|false|null|\d+\.?\d*)(?=\s*[,}\]])', r':"\1"', s)
        return s

    for _ in range(3):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            data = _apply_fixes(data)
    return None


def preprocess_input(data: dict) -> dict:
    """Унифицированная обработка входных данных"""
    data.pop('id', None)  # !!! Удалили id
    def _convert_value(value: Any) -> Any:
        """Рекурсивная конвертация значений"""
        if isinstance(value, dict):
            return {k: _convert_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_convert_value(v) for v in value]
        if isinstance(value, str):
            value = value.strip()

            # Обработка специальных значений
            if not value:
                return None
            if value.lower() in ('null', 'none'):
                return None
            if value.lower() in ('true', 'false'):
                return value.lower() == 'true'

            # Попытка парсинга JSON
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass

            # Конвертация чисел
            try:
                return int(value) if '.' not in value else float(value)
            except ValueError:
                pass

        return value

    return {k: _convert_value(v) for k, v in data.items()}


def remove_empty_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Рекурсивное удаление пустых значений и null"""
    def _is_empty(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, (str, list, dict, bytes)):
            return len(val) == 0
        return False

    def _process(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _process(v) for k, v in value.items() if not _is_empty(v)}
        if isinstance(value, list):
            return [_process(v) for v in value if not _is_empty(v)]
        return value

    return _process(data)