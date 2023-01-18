import time
import logging
import threading
from threading import Timer
from datetime import datetime
from collections import deque

from job import Job
from config import LOGGER_SETTINGS, JSON_FILEPATH
from state_handler import State


logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, pool_size=10, state_handle: State = State()) -> None:
        self.pool_size = pool_size
        self.state_handle = state_handle
        self._queue = deque()
        self._task_loop = deque()
        self._waiting_threads = []
        self._stopped = False

    def _wait_condition(self, task):
        with task.condition:
            task.condition.wait()
            if self._stopped:
                return None
            task.initialize()
            logger.info(str(task))
            self._task_loop.append(task)

    def _posponede_task(self, task):
        """Creates a thread wich waiting notify then add task to the task loop"""
        logger.info(f'{task.name} waiting dependencies' )
        t = threading.Thread(target=self._wait_condition, args=(task,))
        t.start()

    def schedule(self, task: Job) -> None:
        if not self.state_handle.get_state(task.name):
            if len(self._queue) < self.pool_size:
                if task.dependencies:
                    for t in task.dependencies:
                        self._queue.append(t)
                        self._waiting_threads.append(task)
                        self._posponede_task(task)
                else:
                    self._queue.append(task)
            else:
                logger.error('Queue is full')
        else:
            logger.info(f'Task {task.name} already done!')

    def _set_timer(self, task: Job) -> None:
        """Creates a thread that after a specified time adds a task to the task loop"""
        if task.start_at:
            self.state_handle.set_state(key=task.name, value=False)
            interval = (task.start_at - datetime.now()).seconds
            logger.info(str(task))
            t = Timer(interval=interval, function=self._task_loop.append, args=(task,))
            t.start()
            self._waiting_threads.append(t)

    def _initial_with_timer(self, task: Job) -> None:
        """Initialized task which has start at parametr"""
        task.initialize() # тут был неудачный нейминг(task.start()), на самом деле тут просто инициализируется корутина
        self._set_timer(task)

    def _initial_task(self, task: Job) -> Job | None:
        if task.start_at:
            self._initial_with_timer(task)
        else:
            task.initialize()
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
            self.state_handle.set_state(key=task.name, value=False)

    def _check_duration(self, task: Job) -> bool:
        if task.end_at and task.end_at < datetime.now():
            task.stop()
            logger.info(f'{task.name} stopped cause expired time')
            self.state_handle.set_state(key=task.name, value=False)
            return False
        return True

    def _run_task(self, task):
        """
        Gives a command to the coroutine to continue executing the task
        If the coroutine has finished executing, it saves the successful status. 
        If an error occurs during execution, it tries to restart the coroutine.
        """
        try:
            task.run()
            self._task_loop.append(task)
        except StopIteration:
            self.state_handle.set_state(key=task.name, value=True)
            logger.info(f'{task.name} finished')
        except Exception as ex:
            logger.error(f'{task.name} - {ex}')
            self._check_tries(task)

    def _cancel_threads(self):
        """Stops Threads"""
        if self._waiting_threads:
            for t in self._waiting_threads:
                try:
                    t.cancel()
                    logger.info(f'Thread {t} canceled')
                except AttributeError:
                    self.state_handle.set_state(key=t.name, value=False)
                    with t.condition:
                        t.condition.notify() # Отпускает ожидающие потоки, если self.stoped - поток не добавляет task в task_loop

    def run(self) -> None:
        """Starts processing of all tasks from the task loop"""
        self._inialize_all_tasks()
        while True:
            if len(self._task_loop):
                task = self._task_loop.popleft()
                if self._check_duration(task):
                    self._run_task(task)
            # Тут проверяется есть ли еще активные потоки кроме основного, это надо так как если у нас есть 
            # отложеные task на определеное время(те у которых при создании указан start_at), то они будут
            #  добавлены позже к task_loop, для этого через метод
            # _set_timer устанавливается threading.Timer(), который запускает поток по таймеру,
            # который в свою очередь добавляет отложеную по времени task в task_loop, если 
            # не делать эту проверку то после того как все текущие task выполнятся, task_loop 
            # опустеет и выполнение scheduler закончиться до того как отложеная taskа попадет в луп
            elif threading.active_count() > 1: 
                time.sleep(0.3)
                continue
            else:
                logger.info(f'Task loop is empty. Check tasks status in {JSON_FILEPATH}')
                break

    def stop(self):
        self._stopped = True
        logger.info(f'Scheduler was stopped by user. Check tasks status in {JSON_FILEPATH}')
        [self.state_handle.set_state(key=t.name, value=False) for t in self._task_loop]        
        self._cancel_threads()
