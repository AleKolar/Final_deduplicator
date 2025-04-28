import re
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Union, List, Dict


DATE_FORMATS = [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
    '%Y%m%dT%H%M%S',
    '%d.%m.%Y %H:%M:%S',
    '%m/%d/%Y %I:%M %p'
]


class DateTimeParser:
    @classmethod
    def parse(cls, value: Union[str, datetime]) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)

        value = str(value).strip()
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        raise ValueError(f"Unrecognized date format: {value}")


class JSONRepairEngine:
    @staticmethod
    def fix_string(data: str) -> str:
        # Удаление лишних экранирующих символов
        data = re.sub(r"\\+(')", r"\1", data)
        # Балансировка кавычек
        data = re.sub(r"(?<!\\)'(?=[:,\]}])", '"', data)
        # Исправление синтаксиса ключей
        data = re.sub(r'(?<!")(\w+)(?=":)', r'"\1"', data)
        # Добавление пропущенных запятых
        data = re.sub(r'(?<=[}\]"])\s*(?=[{[])', ',', data)
        return data


class DataNormalizer:
    @classmethod
    def deep_clean(cls, data: Any) -> Any:
        def _is_meaningful(value: Any) -> bool:
            return value is not None or (isinstance(value, (str, bytes)) and bool(value.strip()))

        def _process_value(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: _process_value(v) for k, v in value.items() if _is_meaningful(v)}

            if isinstance(value, (list, tuple, set)):
                return [_process_value(item) for item in value if _is_meaningful(item)]

            if isinstance(value, str):
                cleaned = value.strip()

                # Попробуем исправить "неправильные" представления списков
                if cleaned.startswith(",") and cleaned.endswith("[]"):
                    cleaned = cleaned.replace(",", "").strip()  # Убираем лишнюю запятую

                # Обработка JSON-строк, представляющих массивы
                if cleaned.startswith("[") and cleaned.endswith("]"):
                    try:
                        return json.loads(cleaned)  # Пробуем распарсить как JSON
                    except json.JSONDecodeError:
                        pass

                if cleaned and cleaned[0] in ('{', '[', '(') and cleaned[-1] in ('}', ']', ')'):
                    parsed = JSONRepairEngine.fix_string(cleaned)
                    return _process_value(parsed) if parsed != cleaned else cleaned

                if cleaned.lower() in ('null', 'none', 'nil'):
                    return None

                try:
                    return DateTimeParser.parse(cleaned).isoformat()
                except ValueError:
                    pass

            return value

        return _process_value(data)




class ExperimentParser:
    @classmethod
    def parse(cls, value: str) -> List[Dict[str, Any]]:
        if not value:
            return []

        try:
            data = json.loads(value.replace("'", '"'))
            return cls._convert_experiment_values(data)
        except json.JSONDecodeError:
            return cls._parse_legacy_experiments(value)

    @classmethod
    def _parse_legacy_experiments(cls, value: str) -> List[Dict[str, Any]]:
        experiments = []
        buffer = []
        in_quote = False
        for char in value:
            if char == "'":
                in_quote = not in_quote
            if char == ',' and not in_quote:
                experiments.append(':'.join(buffer).strip())
                buffer = []
            else:
                buffer.append(char)
        if buffer:
            experiments.append(':'.join(buffer).strip())

        result = []
        for exp in experiments:
            if ':' in exp:
                k, v = exp.split(':', 1)
                result.append({k.strip(): cls.convert_value(v.strip())})
        return result

    @classmethod
    def _convert_experiment_values(cls, data: Union[List, Dict]) -> Any:
        if isinstance(data, list):
            return [cls._convert_experiment_values(item) for item in data]
        if isinstance(data, dict):
            return {k: cls.convert_value(v) for k, v in data.items()}
        return data

    @staticmethod
    def convert_value(value: Any) -> Any:
        if isinstance(value, str):
            lower_val = value.lower()
            if lower_val == 'true':
                return True
            if lower_val == 'false':
                return False

            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    pass

            if lower_val in ('null', 'none', 'nil'):
                return None

        return value


class JsonSerializer:
    @staticmethod
    def serialize(data: Any) -> str:
        """
        Сериализует объект в JSON-строку с обработкой специальных типов:
        - datetime -> ISO-формат
        - Decimal -> float
        """
        def default_serializer(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, dict):
                return {k: default_serializer(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [default_serializer(item) for item in obj]
            raise TypeError(f"Unserializable type: {type(obj)}")

        return json.dumps(data, default=default_serializer, separators=(",", ":"))