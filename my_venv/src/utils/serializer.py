import json
import base64
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any
from pydantic import BaseModel

from my_venv.src.utils.logger import logger


class JSONSerializer:
    @classmethod
    def serialize_for_hashing(cls, data: Dict[str, Any]) -> bytes:
        """
        Улучшенный сериализатор с расширенной обработкой типов
        """
        def _process_value(value: Any) -> Any:
            if isinstance(value, BaseModel):
                return _process_value(value.model_dump())
            if isinstance(value, dict):
                return {k: _process_value(v) for k, v in value.items()}
            if isinstance(value, (list, tuple, set)):
                return [_process_value(item) for item in value]
            if isinstance(value, datetime):
                return value.isoformat(timespec='microseconds')
            if isinstance(value, bytes):
                return base64.b64encode(value).decode('utf-8')
            if isinstance(value, Decimal):
                return float(value)
            if hasattr(value, 'isoformat'):  # Для date и других datetime-подобных
                return value.isoformat()
            return value

        try:
            processed = _process_value(data)
            return json.dumps(
                processed,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
                default=lambda o: repr(o)  # Страховка для неизвестных типов
            ).encode('utf-8')
        except Exception as e:
            logger.error(f"Serialization error: {str(e)}")
            return json.dumps({"error": str(e)}).encode('utf-8')

    @staticmethod
    def fix_json_string(json_str: str) -> str:
        """
        Улучшенное исправление JSON с обработкой большего числа кейсов
        """
        json_str = json_str.strip()
        if not json_str:
            return json_str

        # Первичная попытка парсинга
        try:
            json.loads(json_str)
            return json_str
        except json.JSONDecodeError:
            pass

        # Исправление основных проблем
        fixes = [
            # 1. Замена неэкранированных одинарных кавычек в значениях
            (r"(?<!\\)'(?=\s*[:}\]])", '"'),

            # 2. Исправление незакрытых кавычек после специальных символов
            (r'([{:,]\s*)\'(?=.)', r'\1"'),

            # 3. Удаление висящих запяток перед закрывающими скобками
            (r',(\s*[}\]])', r'\1'),

            # 4. Обработка неквотированных ключей
            (r'(?<=[{:,])(\s*)(\w+)(\s*:)', r'\1"\2"\3:'),

            # 5. Декодирование hex-последовательнотельности (\xXX)
            (r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16))),

            # 6. Декодирование Unicode-символов (\uXXXX)
            (r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)))
        ]
        # Экранирование специальных символов
        json_str = json_str.replace('\\', '\\\\').replace("None", "null").replace("'", '"')
        return json_str