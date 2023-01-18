import logging
from threading import Condition
from datetime import datetime, timedelta
from typing import Callable

from config import LOGGER_SETTINGS, DT_TEMPLATE


logging.basicConfig(**LOGGER_SETTINGS)
logger = logging.getLogger(__name__)


class Job:
    def __init__(self, *,
                 task: Callable,
                 condition_obj: Condition = Condition(),
                 args: tuple = (),
                 start_at: str = '',
                 max_working_time: int = 0,
                 tries: int = 2,
                 dependencies: list[Callable] = []
                 ) -> None:

        self.start_at = datetime.strptime(start_at, DT_TEMPLATE) if start_at else ''
        self.max_working_time = timedelta(seconds=max_working_time) if max_working_time else 0

        if self.start_at and self.max_working_time:
            self.end_at = self.start_at + self.max_working_time
        elif self.max_working_time:
            self.end_at = datetime.now() + self.max_working_time
        else:
            self.end_at = None

        self.tries = tries
        self.condition = condition_obj
        self.dependencies = [Job(task=t, args=(condition_obj, *args)) for t in dependencies]
        self.task = task
        self.args = args
        self.name = task.__name__
        self._gen = None

    def start(self) -> None:
        self._gen = self.task(*self.args)

    def restart(self):
        if self.tries:
            self._gen = self.task(*self.args)
            self.tries -= 1

    def stop(self) -> None:
        self._gen.close()

    def continue_(self) -> None:
        self._gen.send(True)

    def __str__(self) -> str:
        if not self.start_at:
            return (f'Job {self.name} started at {datetime.now()} end at {str(self.end_at)} tries={self.tries}')
        return (f'Job {self.name} will start at {str(self.start_at)} end at {str(self.end_at)} tries={self.tries}')
