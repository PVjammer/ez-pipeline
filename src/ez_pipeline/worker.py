import concurrent.futures
import logging
import time
import multiprocessing as mp

from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Callable

from .common import Worker, PipelineCallable, PipelineStop
from .decorators import catch_failed_input, acatch_failed_input

logger = logging.getLogger(__name__)
logging.basicConfig(filename="worker.log",encoding='utf-8', level=logging.DEBUG)

class DefaultPipelineWorker(Worker):
    """
    Standard worker which reads data from an input queue, calls a function, and puts the output of that function into 
    an output queue. If the input is a list, each element of the list will be passed to the function sequentially. If the
    function output is a list, each element of the list will be pushed into the output queue sequentially.

    Args:
     function: PipelineFunction which takes as input a single variable `input` and produces an output. Use `functools.partial` 
               when creating a PipelineFunction to include other parameters or pass a dictionary as input.
     input_queue: Queue to use for generating input. Default Pipeline implementation uses a `multiprocessing.Queue`
     output_queue: Queue to use for pushing output. Default Pipeline implementation uses a `multiprocessing.Queue`
    """
    def __init__(self, 
                 function: Callable, 
                 input_queue: Queue, 
                 output_queue: Queue, 
                 num_workers: int = 1,
                 *args, 
                 **kwargs):
        super().__init__(function, input_queue, output_queue,  num_workers=num_workers, *args, **kwargs)
        logger.debug(f"Worder initialized with params {self.__dict__}")

    def _get_input(self):

        if self._input_q.empty():
            return None

        _input = self._input_q.get()
        logger.info(f"{self._function.name} got input {_input}")

        if not self._check_valid_inputs(_input):
            return None

        return _input

    def _process_input(self, input):
        if isinstance(input, list) and not self.batch:
            logger.warning(f"{self._function.name} got list as input")
            for i in input:
                o, err = _wrap(self._function, i)
                if not err:
                    logger.debug(f"{self._function.name} inserting {o} into output queue")
                    self._output_q.put(o)
            
        else:
            o, err = _wrap(self._function, input)
            logger.debug(f"{self._function.name} inserting {o} into output queue")
            if isinstance(o, list):
                for _o in o:
                    if not err:
                        self._output_q.put(_o)
            else:
                if not err:
                    self._output_q.put(o)
        # print(err.input_dict, not err)
        if self.err_queue and err:
            # print(f"{err.__dict__}")
            self.err_queue.put(err)

    def _exec(self, thread_number: int = 0):     
        logger.debug(f"{self._function.name} is running synchronously")
        while True:
            if self.batch:
                input = self._pool_data()
            else:
                input = self._get_input()
            logging.info(f"{self._function.name}:thread-{thread_number} got input {input}")
            if isinstance(input, PipelineStop):
                logging.info(f"{self._function.name}:thread-{thread_number} stopping")
                self._input_q.put(PipelineStop())
                break
            if not input:
                time.sleep(0.1)
                continue
            # try:
            self._process_input(input)
            # except:
            #     logger.exception(f"{self._function.name} Exception when processing input: {input}")

    def _pool_data(self) -> list | PipelineStop:
        data = []
        _input = self._get_input()

        logger.debug(f"{self._function.name} got initial input {_input}")
        if isinstance(_input, PipelineStop):
            logger.warning(f"{self._function.name} got pipelineStop during pooling")
            return PipelineStop()
        if _input is not None:
            data.append(_input)

        while len(data) < self.batch_size:
            _input = self._get_input()
            if not _input:
                logger.debug(f"{self._function.name} got `None` for input during pooling")
                break
                
            elif isinstance(_input, PipelineStop):
                self._input_q.put(PipelineStop())
                break

            data.append(_input)
        logger.info(f"{self._function.name} pooling returning list of length {len(data)}")
        return data

    def _exec_single_threaded(self):
        self._exec()
        self._output_q.put(PipelineStop())

    def _exec_threaded(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            for i in range(self._num_workers):
                executor.submit(self._exec, i)
        self._output_q.put(PipelineStop()) 
                
    async def _aexec(self):
        logger.debug(f"{self._function.name} is running with async")
        while True:
            if self.batch:
                input = self._pool_data()
            else:
                input = self._get_input()
            logging.info(f"{self._function.name} got input {input}")
            if isinstance(input, PipelineStop):
                logging.info(f"{self._function.name} stopping")
                self._input_q.put(PipelineStop())
                break
            if not input:
                time.sleep(0.1)
                continue
            
            await self._aprocess_input(input)
            # except:
            #     logger.exception(f"{self._function.name} Exception when processing input: {input}")
            # finally:
            #     self._output_q.put(PipelineStop)
            #     break
        self._output_q.put(PipelineStop())   

    async def _aprocess_input(self, input):
        if isinstance(input, list) and not self.batch:
            logger.info(f"{self._function.name} flattening list of length {len(input)}")
            for i in input:
                o, err = await _awrap(self._function, i)
                if isinstance(o, list):
                    logger.debug(f"{self._function.name} inserting batch of length {len(o)} into output queue")    
                else:
                    logger.debug(f"{self._function.name} inserting {o} into output queue")
                self._output_q.put(o)
            
        else:
            logger.info(f"{self._function.name} passing batch to function")
            o, err = await _awrap(self._function, input)
            
            if isinstance(o, list) and not self.batch:
                for _o in o:
                    logger.debug(f"{self._function.name} inserting {_o} into output queue")
                    self._output_q.put(_o)
            else:
                logger.debug(f"{self._function.name} has output of type {type(o)}" )
                if isinstance(o, list):
                    logger.debug(f"{self._function.name} inserting batch of length {len(o)} into output queue")    
                else:
                    logger.debug(f"{self._function.name} inserting {o} into output queue")
                self._output_q.put(o)
        # print(err.input_dict, not err)
        if self.err_queue and err:
            self.err_queue.put(err)


def _wrap(function: Callable, input: Any):    
    # Case: Input is not a dictionary
    if not isinstance(input, dict):
        _output, err = catch_failed_input(function)(input)
        if not function.output_variable:    
            return _output, err
        return {function.output_variable: _output}, err

    # Input is a dictionary. 
    # Case: No input variable specified.
    if not function.input_variable:
        logger.debug(f"No input variable specifed. Passing {input}")
        _output, err = catch_failed_input(function)(input)
        if not function.output_variable:    
            return _output, err
        return {function.output_variable: _output}, err

    # Case: Input variable is specified.
    _input =  input.get(function.input_variable, None)
    logger.debug(f"Got {_input} from input dictionary.")
    
    if  _input is None:
        logger.warn(f"Variable {function.input_variable} not present in input. Passing the input instead. {input}")
        _output, err = catch_failed_input(function)(input)
    else:
        _output, err = catch_failed_input(function)(_input)
    
    if function.output_variable is None:    
        return _output, err
    input.update({function.output_variable: _output})

    return input, err

async def _awrap(function: Callable, input: Any):    
    # Case: Input is not a dictionary
    if not isinstance(input, dict):
        _output, err = await acatch_failed_input(function)(input)
        if not function.output_variable:    
            return _output, err
        return {function.output_variable: _output}, err

    # Input is a dictionary. 
    # Case: No input variable specified.
    if not function.input_variable:
        logger.debug(f"No input variable specifed. Passing {input}")
        _output, err = await acatch_failed_input(function)(input)
        if not function.output_variable:    
            return _output, err
        return {function.output_variable: _output}, err

    # Case: Input variable is specified.
    _input =  input.get(function.input_variable, None)
    logger.debug(f"Got {_input} from input dictionary.")
    
    if  _input is None:
        logger.warn(f"Variable {function.input_variable} not present in input. Passing the input instead. {input}")
        _output, err =  await acatch_failed_input(function)(input)
    else:
        _output, err =  await acatch_failed_input(function)(_input)
    
    if function.output_variable is None:    
        return _output, err
    input.update({function.output_variable: _output})

    return input, err

async def _arun_sync(f: Callable, input: Any):
    return f(input)

# def catch_failed_output(func: Callable, input: Any):
#     output = None
#     err = None