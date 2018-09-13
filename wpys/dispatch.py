from uuid import uuid4

from .config import WPySConfig
from .encoding import (
    Capabilities, ProcessOfferings, StatusInfo, Result, ExceptionReport, encode_response
)
from .parsing import (
    GetCapabilitiesRequest, DescribeProcessRequest, ExecuteRequest,
    GetStatusRequest, GetResultRequest, DismissRequest
)
from .job import JobStatus


__all__ = ['dispatch']

# the request handler registry
REQUEST_HANDLERS = {}

def handles(request_type):
    """ Decorator to register a function to handle a specific request type
    """
    def _inner(fn):
        REQUEST_HANDLERS[request_type] = fn
        return fn
    return _inner


@handles(GetCapabilitiesRequest)
async def handle_get_capabilities(process_registry, broker, config, wps_request):
    service_info = config.service_info
    return Capabilities(
        title=service_info.title,
        abstract=service_info.abstract,
        keywords=service_info.keywords,
        fees=service_info.fees,
        access_constraints=service_info.access_constraints,

        provider_name=service_info.provider_name,
        provider_site=service_info.provider_site,
        individual_name=service_info.individual_name,
        electronical_mail_address=service_info.electronical_mail_address,

        service_endpoint=config.main_endpoint_name,
        processes=process_registry.processes,
    )


@handles(DescribeProcessRequest)
async def handle_describe_process(process_registry, broker, config, wps_request):
    return ProcessOfferings(processes=process_registry.processes)


@handles(ExecuteRequest)
async def handle_execute(process_registry, broker, config, wps_request):
    process = process_registry.get_process(wps_request.identifier)
    job = await broker.create_job(
        str(uuid4()), process, wps_request.inputs, wps_request.outputs
    )
    await broker.enqueue_job(job.identifier)
    if wps_request.mode == "async":
        return StatusInfo.from_job(job)
    else:
        while True:
            await broker.get_job_notification(
                job.identifier, ["succeded", "failed", "dismissed"]
            )
            job = await broker.get_job(job.identifier)
            if job.status == JobStatus.SUCCEEDED:
                return Result.from_job(job)
            elif job.status == JobStatus.FAILED:
                return ExceptionReport.from_job(job)
            else:
                return StatusInfo.from_job(job)


@handles(GetStatusRequest)
async def handle_get_status(process_registry, broker, config, wps_request):
    job = await broker.get_job(wps_request.job_id)
    return StatusInfo.from_job(job)


@handles(GetResultRequest)
async def handle_get_result(process_registry, broker, config, wps_request):
    pass


@handles(DismissRequest)
async def handle_dismiss(process_registry, broker, config, wps_request):
    await broker.dismiss_job(wps_request.job_id)
    return StatusInfo.from_job(await broker.get_job(wps_request.job_id))


async def dispatch(process_registry, broker, config: WPySConfig, wps_request):
    """ The main request dispatching function.
    """

    try:
        result = await REQUEST_HANDLERS[type(wps_request)](
            process_registry, broker, config, wps_request
        )
        return encode_response(result, config), 200, {'Content-Type': 'application/xml'}
    except Exception as e:
        return encode_response(
            ExceptionReport.from_exception(e, config), config
        ), 400, {'Content-Type': 'application/xml'}
