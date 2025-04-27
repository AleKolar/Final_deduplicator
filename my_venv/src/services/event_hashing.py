import hashlib
from typing import Dict, Any

from my_venv.src.utils.serializer import ExperimentParser, DataNormalizer, JsonSerializer


class HashGenerationError(Exception):
    pass

class EventHashService:
    @staticmethod
    def generate_unique_fingerprint(data: Dict[str, Any], event_name: str) -> str:
        """Генерация хеша с учетом всех нормализаций"""
        try:
            # 1. Нормализация данных с использованием DataNormalizer
            normalized_data = DataNormalizer.deep_clean(data)

            # 2. Фильтрация значений и их конвертация
            filtered_data = {
                k: ExperimentParser.convert_value(v)  # Применение конвертации в зависимости от типа
                for k, v in normalized_data.items()
                if ExperimentParser.convert_value(v) is not None
            }

            # 3. Специальная обработка experiments
            if 'raw_data' in filtered_data and 'experiments' in filtered_data['raw_data']:
                experiments = filtered_data['raw_data']['experiments']
                if isinstance(experiments, (list, dict)):
                    filtered_data['raw_data']['experiments'] = sorted(
                        experiments.items() if isinstance(experiments, dict) else experiments,
                        key=lambda x: str(x)
                    )

            # 4. Сериализация данных для создания хеша
            payload = {
                "event": event_name,
                "fields": dict(sorted(filtered_data.items()))
            }

            # 5. Генерируем хеш
            return hashlib.sha3_256(
                JsonSerializer.serialize(payload).encode()
            ).hexdigest()

        except Exception as e:
            raise HashGenerationError(f"Fingerprint error: {str(e)}")