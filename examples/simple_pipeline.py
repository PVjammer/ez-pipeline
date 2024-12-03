import math
from ez_pipeline import PipelineFunction

def add_one(input: int) -> int:
    return input + 1

def double(input: int) -> int:
    return input * 2

def s_root(input: int) -> float:
    return math.sqrt(input)


if __name__ == "__main__":
    pipeline = PipelineFunction(add_one) | double | s_root

    input_list = list(range(10000))

    ans = pipeline.run(input_list)
    print(f"{len(ans)=},  {ans[0]=},  {ans[-1]=}")