import random

import fire


def execute_test_pipeline(**_):
    crash_probability = 0.1
    crash_roll = random.random()
    if crash_roll < crash_probability:
        raise EnvironmentError("simulated pipeline crash")
    success_probability = 0.8
    pipeline_roll = random.random()
    if pipeline_roll < success_probability:
        return "return code: successful"
    return "return code: failure"


if __name__ == '__main__':
    fire.Fire(execute_test_pipeline)
