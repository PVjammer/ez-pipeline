import functools
import logging

# import pandas as pd
from typing import Any, Callable

logger = logging.getLogger(__name__)

class Err:

    def __init__(self, input_dict: dict):
        self.input_dict = input_dict
    
    def get(self, key: str) -> Any:
        return self.input_dict.get(key)
    
    def __bool__(self):
        return self.input_dict not in [{}, None]
    


# def _get_batch_from_list(_list: list, batch_size: int) -> list[list]:
#     output_list = [_list[i*batch_size:(i+1)*batch_size] for i in range(len(_list)//batch_size)]
#     if len(_list) % batch_size != 0:
#         output_list.append(_list[(len(_list)//batch_size)*batch_size:])
#     return output_list

# def _get_batches_from_df(df: pd.DataFrame, batch_size: int, as_list):
#     batches = [df[i*batch_size:(i*batch_size+batch_size)] for i in range(len(df)//batch_size)]
#     if len(df) % batch_size > 0:
#         batches.append(df[(len(df)//batch_size)*batch_size:])
#     if as_list:
#         return [df.to_dict(orient="records") for df in batches]
    
#     return batches

# def batch_generator(batch_size=None, as_list=True):
#     def batch_generator_decorator(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             output = func(*args, **kwargs)
            
#             if isinstance(output, list):
#                 batches =  _get_batch_from_list(output, batch_size)
#             elif isinstance(output, pd.DataFrame):
#                 batches =  _get_batches_from_df(output, batch_size, as_list)
#             else:
#                 raise ValueError(f"Cannot create batches from object of type {type(output)}")
#             for batch in batches:
#                 yield batch
#         return wrapper
#     return batch_generator_decorator


def catch_failed_input(func: Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        output = None
        err = None
        try:
            output = func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Error processing input: {e}")
            err = {"args": [a for a in args], "kwargs": kwargs, "exception": e}
        finally:
            return output, Err(err)
    return wrapper

def acatch_failed_input(func: Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        output = None
        err = None
        try:
            output = await func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Error processing input: {e}")
            err = {"args": [a for a in args], "kwargs": kwargs, "exception": e}
        finally:
            return output, Err(err)
    return wrapper

@catch_failed_input
def test_func(num: float, denom: float, **kwargs):
    return num / denom

if __name__ == "__main__":
    ans, err = test_func(2.0, 5.0)
    if err:
        print("There's an error")
    print(ans, err)
    
    
    ans, err = test_func(4.0, 0, url="My-Url")
    if err:
        print("There's an error")
    print(ans, err)
