import os
import json
import logging
from typing import Any

from config import LOGGER_SETTINGS, JSON_FILEPATH

logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


class JsonFileStorage:
    def __init__(self, file_path: str = JSON_FILEPATH):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        try:
            with open(self.file_path, 'w') as file:
                json.dump(state, file)
        except Exception as ex:
            logger.error(ex)

    def retrieve_state(self) -> dict:
        if os.path.isfile(self.file_path):
            try:
                with open(self.file_path, 'r') as file:
                    data = json.load(file)
                    return data
            except Exception as ex:
                logger.error(ex)
        return {}

    def clear_data(self):
        if os.path.isfile(self.file_path):
            os.remove(self.file_path)


class State:
    """
    The class for storing state when working with data, so that
    do not constantly reread the data from the beginning.
    Here is a stateful-to-file implementation.
    """

    def __init__(self, storage: JsonFileStorage = JsonFileStorage()):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Set state for a specific key"""
        old_data = self.storage.retrieve_state()
        data = {key: value}
        if old_data:
            data = old_data | data
        self.storage.save_state(data)

    def get_state(self, key: str, default: str = None) -> str:
        """Get state by specific key"""
        data = self.storage.retrieve_state()
        if data.get(key, None):
            return data.get(key)
        return default

    def clear_statement(self):
        self.storage.clear_data()
