import os
import time
import json
import logging
from threading import Condition
from datetime import datetime, timedelta

from scheduler import Scheduler
from job import Job
from config import LOGGER_SETTINGS, CITIES
from utils import coro_initializer
from task_files.api_client import YandexWeatherAPI, DataFetching


logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


@coro_initializer
def read_text():
    _ = (yield)
    with open('task_files/lorem.txt', 'r') as file:
        while text := file.readline():
            _ = (yield)
            yield text


@coro_initializer
def create_dirs(cv: Condition, dir_count: int):
    _ = (yield)
    for i in range(dir_count):
        _ = (yield)
        os.makedirs(f'dir{i}')
    with cv:
        cv.notify_all()


@coro_initializer
def delete_dirs(dir_count: int):
    _ = (yield)
    for i in range(dir_count):
        _ = (yield)
        os.removedirs(f'dir{i}')


@coro_initializer
def get_weather_forecast():
    yw_api = YandexWeatherAPI()
    data_fetcher = DataFetching(yw_api, CITIES.keys())
    _ = (yield)
    with open('forcast.json', 'a+') as file:
        for data in data_fetcher.get():
            _ = (yield)
            json.dump(data, file)  


start_at = str((datetime.now() + timedelta(seconds=3)))
scheduler = Scheduler()
task1 = Job(task=read_text, tries=3, start_at=start_at, max_working_time=0)
task2 = Job(task=delete_dirs, dependencies=[create_dirs], args=(100,))
task3 = Job(task=get_weather_forecast)


if __name__ == '__main__':
    logger.info('Scheduler started')
    [scheduler.schedule(t) for t in (task1, task2, task3)]
    try:
        scheduler.run()
    except KeyboardInterrupt:
        scheduler.stop()
    logger.info('Scheduler finished')
