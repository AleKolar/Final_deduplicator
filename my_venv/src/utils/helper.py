from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def remove_empty_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Безопасное удаление пустых значений"""
    def _is_empty(value: Any) -> bool:
        if isinstance(value, (list, dict, str, bytes)):
            return len(value) == 0
        return value is None

    def _process(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _process(v) for k, v in value.items() if not _is_empty(v)}
        if isinstance(value, list):
            return [_process(item) for item in value if not _is_empty(item)]
        return value

    return _process(data)

def _preprocess_value(value: Any) -> Any:
    try:
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        if isinstance(value, dict):
            return {k: _preprocess_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_preprocess_value(item) for item in value]
        return value
    except Exception as e:
        logger.warning(f"Preprocessing error: {str(e)}")
        return value