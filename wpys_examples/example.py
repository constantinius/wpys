from datetime import timedelta
import time

from wpys.process import process
from wpys.job import Status, Result

def feet_to_meter(v):
    return v * 0.3048

@process(
    outputs=[{
        "identifier": "distance",
        "domains": [{
            "uom": "meter",
            "data_type": "http://www.w3.org/2001/XMLSchema#double",
        }, {
            "uom": "feet",
            "data_type": "http://www.w3.org/2001/XMLSchema#double",
            "to_default_domain": feet_to_meter
        }],
        "formats": [
            {"mimetype": "text/plain"},
            {"mimetype": "text/xml"},
        ]
    }]
)
def long_running_process(sleep_time: int=5) -> int:
    """ This is a long, long, loooong running process. In fact you can choose how long.
    """
    time.sleep(sleep_time / 3)
    yield Status(percent_completed=33, next_poll=timedelta(seconds=sleep_time / 3), estimated_completion=timedelta(seconds=sleep_time / 3 * 2))
    time.sleep(sleep_time / 3)
    yield Status(percent_completed=66, next_poll=timedelta(seconds=sleep_time / 3), estimated_completion=timedelta(seconds=sleep_time / 3))
    time.sleep(sleep_time / 3)
    yield Status(percent_completed=100)
    yield Result(42)

    raise Exception("Lolol")
