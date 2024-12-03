import logging
from functools import partial
from typing import Any, Callable
from .pipeline import PipelineFunction



def _default_logging_function(input: Any, logger: logging.Logger, logging_level: str = "info", stdout: bool = False, name = "LogStage"):
    logging_map = {
        "info": logger.info,
        "debug": logger.debug,
        "warn": logger.warn,
    }    

    logging_func = logging_map.get(logging_level, logger.info)

    logging_string = f"LogStage for input {input}"

    if stdout:
        print(logging_string)
    logging_func(logging_string)

    return input


class PipelineUtils(PipelineFunction):

    @staticmethod
    def Logger(name: str, logger: logging.Logger = None, logging_level: str = "info", logging_function: Callable = None, **kwargs):

        if not logging_function:
            if not logger:
                logger = logging.getLogger(__name__)
            logging_function = partial(_default_logging_function, logger=logger, logging_level=logging_level, **kwargs)
        
        return PipelineFunction(
            name=name, 
            function=logging_function
        )