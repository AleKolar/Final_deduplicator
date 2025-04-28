import hashlib
import json
from typing import Dict, Any
import uuid
from datetime import datetime

from my_venv.src.utils.serializer import DataNormalizer


class HashGenerationError(Exception):
    pass


class EventHashService:
    @staticmethod
    def generate_unique_fingerprint(data: Dict[str, Any], event_name: str) -> str:
        """
        Генерация уникального хеша для события.
        """
        try:
            # Создаем копию и добавляем гарантированно уникальные метки (новый UUID)
            processed_data = data.copy()
            processed_data['__event_uuid'] = str(uuid.uuid4())
            processed_data['__timestamp'] = datetime.now().isoformat()  # Добавляем текущее время

            normalized_data = DataNormalizer.deep_clean(processed_data)

            filtered_data = {
                k: v for k, v in normalized_data.items()
                if not EventHashService._is_empty(v)
            }

            payload = {
                "event": event_name,
                "data": dict(sorted(filtered_data.items()))
            }

            # import logging
            # logging.debug(f"Hash payload: {payload}")

            return hashlib.sha3_256(
                json.dumps(payload, sort_keys=True, ensure_ascii=False).encode('utf-8')
            ).hexdigest()

        except Exception as e:
            raise HashGenerationError(f"Fingerprint generation failed: {str(e)}")


    @staticmethod
    def _is_empty(value) -> bool:
        """Проверка на полностью пустое значение"""
        if value is None:
            return True
        if isinstance(value, str):
            return len(value.strip()) == 0 or value in [",[]", "[]"]
        if isinstance(value, (list, dict, set)):
            return len(value) == 0
        return False


