import json
import logging
from typing import List

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    """Ініціалізує адаптер з базовим URL API."""
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]) -> bool:
        # Конвертація списку даних агентів в формат JSON
        data = [json.loads(item.json()) for item in processed_agent_data_batch]
        # Відправка POST запиту до API для збереження оброблених даних
        result = requests.post(f"{self.api_base_url}/processed_agent_data/",  json=data)
        # Перевірка статусу відповіді на успішність (200 OK)
        return result.status_code == requests.codes.ok