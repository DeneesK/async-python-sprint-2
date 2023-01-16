import time
import logging
from datetime import datetime, timedelta

from scheduler import Scheduler
from job import Job
from config import LOGGER_SETTINGS
from utils import coro_initializer


logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


@coro_initializer
def task1(sleep_time: int = 0.2):
    flag = (yield)
    with open('new.txt', 'r') as file:
        while flag:
            flag = (yield)
            text = file.readline()
            if not text:
                return
            time.sleep(sleep_time)


@coro_initializer
def task2(sleep_time: int = 0.1):
    flag = (yield)
    with open('new2.txt', 'r') as file:
        while flag:
            flag = (yield)
            text = file.readline()
            if not text:
                return
            time.sleep(sleep_time)


@coro_initializer
def task3(sleep_time: int = 0.2):
    flag = (yield)
    with open('new3.txt', 'r') as file:
        while flag:
            flag = (yield)
            text = file.readline()
            if not text:
                return
            time.sleep(sleep_time)


job1 = Job(task=task1, start_at=str(datetime.now() + timedelta(seconds=7)), tries=3)
job2 = Job(task=task2, max_working_time=4, tries=3)
job3 = Job(task=task3, tries=3)
schedule = Scheduler()
schedule.schedule(job1)
schedule.schedule(job2)
schedule.schedule(job3)


if __name__ == '__main__':
    logger.info('Schedule programm started')

    try:
        schedule.run()
    except KeyboardInterrupt:
        schedule.stop()

    logger.info('Schedule programm finished')
