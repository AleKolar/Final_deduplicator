import hashlib
import orjson
from typing import Dict, Any

from my_venv.src.utils.data_cleaners import deep_clean


class EventHashService:
    @staticmethod
    def generate_unique_fingerprint(raw_data: Dict[str, Any]) -> str:
        """
        Генерация хеша только для непустых значений
        с автоматическим удалением технических полей
        """
        try:
            # 1. Глубокая очистка данных
            cleaned_data = deep_clean(raw_data)

            # 2. Проверка после очистки
            if not cleaned_data:
                return hashlib.sha3_256(orjson.dumps({})).hexdigest()  # Хеш пустого объекта

            # 3. Рекурсивная сортировка
            def deep_sort(obj):
                if isinstance(obj, dict):
                    return {k: deep_sort(v) for k, v in sorted(obj.items())}
                if isinstance(obj, list):
                    return sorted(deep_sort(item) for item in obj)
                return obj

            sorted_data = deep_sort(cleaned_data)

            # 4. Сериализация
            serialized = orjson.dumps(
                sorted_data,
                option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS
            )

            return hashlib.sha3_256(serialized).hexdigest()

        except Exception as e:
            raise RuntimeError(f"Hash generation error: {str(e)}")

