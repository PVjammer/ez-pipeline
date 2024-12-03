import asyncio
import time

from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Callable

class PipelineStop:
    pass


class PipelineCallable(ABC):

    @abstractmethod
    def _exec(self, input: Any):
        raise NotImplementedError

    def __call__(self, input):
        return self._exec(input)


class Worker(ABC):
    """"""
    def __init__(self, 
                 function: Callable,
                 input_queue: Queue,
                 output_queue: Queue,
                 valid_inputs: list = None,
                 num_workers: int = 1,
                 batch_size: int = None,
                 is_async: bool = False,
                 err_queue: Queue = None,
                 *args, 
                 **kwargs):
        self._input_q = input_queue
        self._output_q = output_queue
        self._function = function
        self._num_workers = num_workers
        self.valid_inputs = None if not valid_inputs or len(valid_inputs) < 1 else valid_inputs
        self.batch_size = batch_size if isinstance(batch_size, int) else None
        self.batch = False if not self.batch_size else True
        self.is_async = is_async
        self.err_queue = err_queue

    @abstractmethod
    def _exec(self, *args, **kwargds):
        raise NotImplementedError
    
    def _exec_single_threaded(self):
        raise NotImplementedError
    
    @abstractmethod
    def _exec_threaded(self):
        raise NotImplementedError

    def _check_valid_inputs(self, input: Any):
        if not input:
            time.sleep(0.1)
            return False

        if not self.valid_inputs:
            return True

        for v in self.valid_inputs:
            if isinstance(input, v):
                return True

        return False

    def _aexec(self):
        raise NotImplementedError
    
    def run(self):
        if self.is_async:
            print("Setting up worker to run with async")
            asyncio.run(self._aexec())
        elif self._num_workers > 1:
            self._exec_threaded()
        else:
            self._exec_single_threaded()
    