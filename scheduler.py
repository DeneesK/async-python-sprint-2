import time
import logging
import threading
from threading import Timer
from datetime import datetime
from collections import deque

from job import Job
from config import LOGGER_SETTINGS
from state_handler import State


logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, pool_size=10, state_handle: State = State()) -> None:
        self.pool_size = pool_size
        self.state_handle = state_handle
        self._queue = deque()
        self._queue_copy = deque()
        self._task_loop = deque()
        self._waiting_threads = []

    def schedule(self, task: Job) -> None:
        self.state_handle.clear_statement()
        if len(self._queue) < self.pool_size:
            self._queue.append(task)
            self._queue_copy.append(task)
        else:
            logger.error('Queue is full')

    def _set_timer(self, task: Job) -> None:
        if task.start_at:
            interval = (task.start_at - datetime.now()).seconds
            logger.info(str(task))
            t = Timer(interval=interval, function=self._task_loop.append, args=(task,))
            t.start()
            self._waiting_threads.append(t)

    def _initial_with_timer(self, task: Job) -> None:
        task.start()
        self._set_timer(task)

    def _initial_task(self, task: Job) -> Job | None:
        if task.start_at:
            self._initial_with_timer(task)
        else:
            task.start()
            logger.info(str(task))
            return task

    def _inialize_all_tasks(self):
        while True:
            if len(self._queue):
                task = self._queue.popleft()
                initialized_task = self._initial_task(task)
                if initialized_task:
                    self._task_loop.append(initialized_task)
            else:
                logger.info('All tasks from queue added to task loop')
                break

    def _check_tries(self, task: Job) -> None:
        if task.tries:
            logger.info(f'{task.name} will be restarted, restart attempts = {task.tries}')
            task.restart()
            self._task_loop.append(task)
        else:
            logger.info(f'{task.name} will be stopped cause run out of attempts to restart')

    def _check_duration(self, task: Job) -> bool:
        if task.end_at and task.end_at < datetime.now():
            try:
                task.stop()
            except StopIteration:
                logger.info(f'{task.name} stopped cause expired time')
                return False
        return True

    def _job_continue(self, task):
        try:
            task.continue_()
            self._task_loop.append(task)
        except StopIteration:
            self.state_handle.set_state(key=task.name, value=True)
            logger.info(f'{task.name} finished')
        except Exception as ex:
            logger.error(f'{task.name} - {ex}')
            self._check_tries(task)

    def _cancel_threads(self):
        if self._waiting_threads:
            for t in self._waiting_threads:
                t.cancel()
                logger.info(f'Thread {t} canceled')

    def run(self) -> None:
        self._inialize_all_tasks()
        while True:
            if len(self._task_loop):
                task = self._task_loop.popleft()
                if self._check_duration(task):
                    self._job_continue(task)
            elif threading.active_count() > 1:
                time.sleep(0.3)
                continue
            else:
                logger.info('Task loop is empty. Check tasks status in state.json')
                break

    def restart(self):
        logger.info('Shceduler will be restarted')
        self._cancel_threads()
        self._queue.clear()
        self._task_loop.clear()

        for task in self._queue_copy:
            if not self.state_handle.get_state(task.name):
                self._queue.append(task)
        self.run()

    def stop(self):
        logger.info('Scheduler was stopped by user')
        self._cancel_threads()
        answer = input('Continue y/n: ')

        match answer.lower():
            case 'y':
                self.restart()
            case 'n':
                logger.info('Scheduler was closed by user')
                exit(0)
            case _:
                print('Input "y" or "n"')
                self.stop()
