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

    def _wait_condition(self, task):
        with task.condition:
            task.condition.wait()
            task.start()
            logger.info(str(task))
            self._task_loop.append(task)

    def _posponede_task(self, task):
        """Creates a thread whose task, waiting notify then to add task to the task loop"""
        logger.info(f'{task.name} waiting dependencies' )
        t = threading.Thread(target=self._wait_condition, args=(task,))
        t.start()

    def schedule(self, task: Job) -> None:
        self.state_handle.clear_statement()
        if len(self._queue) < self.pool_size:
            if task.dependencies:
                for t in task.dependencies:
                    self._queue.append(t)
                    self._queue_copy.append(t)
                    self._posponede_task(task)
            else:
                self._queue.append(task)
                self._queue_copy.append(task)
        else:
            logger.error('Queue is full')

    def _set_timer(self, task: Job) -> None:
        """Creates a thread that after a specified time adds a task to the task loop"""
        if task.start_at:
            interval = (task.start_at - datetime.now()).seconds
            logger.info(str(task))
            t = Timer(interval=interval, function=self._task_loop.append, args=(task,))
            t.start()
            self._waiting_threads.append(t)

    def _initial_with_timer(self, task: Job) -> None:
        """Initialized task which has start at parametr"""
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
        """Checks how many restarts the task has left if all attempts are used, stops the task"""
        if task.tries:
            logger.info(f'{task.name} will be restarted, restart attempts = {task.tries}')
            task.restart()
            self._task_loop.append(task)
        else:
            logger.info(f'{task.name} will be stopped cause run out of attempts to restart')

    def _check_duration(self, task: Job) -> bool:
        if task.end_at and task.end_at < datetime.now():
            task.stop()
            logger.info(f'{task.name} stopped cause expired time')
            return False
        return True

    def _run_task(self, task):
        """
        Gives a command to the coroutine to continue executing the task
        If the coroutine has finished executing, it saves the successful status. 
        If an error occurs during execution, it tries to restart the coroutine.
        """
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
        """Stops Timer Thread"""
        if self._waiting_threads:
            for t in self._waiting_threads:
                t.cancel()
                logger.info(f'Thread {t} canceled')

    def run(self) -> None:
        """Starts processing of all tasks from the task loop"""
        self._inialize_all_tasks()
        while True:
            if len(self._task_loop):
                task = self._task_loop.popleft()
                if self._check_duration(task):
                    self._run_task(task)
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
