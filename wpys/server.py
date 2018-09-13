from datetime import datetime, timedelta
import asyncio

from quart import Quart, request

from .parsing import parse_xml_request, parse_kvp_request
from .dispatch import dispatch
from .config import load_config
from .registry import load_process_registry
from .broker import get_broker
from .backend import get_result_backend

app = Quart(__name__)
config = load_config()


@app.route(config.main_endpoint_name, methods=['GET', 'POST'])
async def endpoint():
    if request.method == 'GET':
        wps_request = parse_kvp_request(request.args)
    elif request.method == 'POST':
        data = bytes(await request.get_data())
        print(type(data))
        wps_request = parse_xml_request(data)

    broker = await get_broker(config, asyncio.get_event_loop())
    process_registry = load_process_registry(config)

    result = await dispatch(process_registry, broker, config, wps_request)
    return result


@app.route(config.result_endpoint_name, methods=['GET'])
async def result_endpoint(job_id, result_name):
    result_backend = get_result_backend(config)
    raw_result = await result_backend.get_job_result(job_id, result_name)

    async def raw_result_iterator(raw_result):
        while True:
            chunk = await raw_result.read(config)
            if not chunk:
                break
            yield chunk

    return raw_result_iterator(raw_result)

