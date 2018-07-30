from datetime import datetime, timedelta

from quart import Quart, request

from .parsing import parse_xml_request, parse_kvp_request
from .dispatch import dispatch
from .registry import ProcessRegistry
from .process import process
from .job import JobManager, Status, Result

app = Quart(__name__)


import time


@process(
    outputs=[{
        "identifier": "distance",
        "domains": [{
            "uom": "meter",
            "data_type": "http://www.w3.org/2001/XMLSchema#double",
        }, {
            "uom": "feet",
            "data_type": "http://www.w3.org/2001/XMLSchema#double",
            "to_default_domain": lambda v: v * 0.3048
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

process_registry = ProcessRegistry()
process_registry.register(long_running_process)

job_manager = JobManager()

@app.route('/', methods=['GET', 'POST'])
async def endpoint():
    if request.method == 'GET':
        wps_request = parse_kvp_request(request.args)
    elif request.method == 'POST':
        data = await request.get_data()
        wps_request = parse_xml_request(data.decode('utf-8'))

    result = await dispatch(process_registry, job_manager, wps_request)
    return result
