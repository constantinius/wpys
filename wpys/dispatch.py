from .encoding import (
    Capabilities, ProcessOfferings, StatusInfo, encode_response
)
from .parsing import (
    GetCapabilitiesRequest, DescribeProcessRequest, ExecuteRequest,
    GetStatusRequest, GetResultRequest, DismissRequest
)
__all__ = ['dispatch']

REQUEST_HANDLERS = {}

def handles(request_type):
    def _inner(fn):
        REQUEST_HANDLERS[request_type] = fn
        return fn
    return _inner


@handles(GetCapabilitiesRequest)
async def handle_get_capabilities(process_registry, job_manager, wps_request):
    return Capabilities(
        title="testtitle",
        abstract="abstract",
        keywords=['some', 'test', 'keywords'],
        fees="a lot",
        access_constraints="not many",

        provider_name="cool corp. inc",
        provider_site='http://coolcorp.com',
        individual_name="Stevie McManager",
        electronical_mail_address="stevie@coolcorp.com",

        processes=[]
    )


@handles(DescribeProcessRequest)
async def handle_describe_process(process_registry, job_manager, wps_request):
    return ProcessOfferings(processes=process_registry.processes)

@handles(ExecuteRequest)
async def handle_execute(process_registry, job_manager, wps_request):
    process = process_registry.get_process(wps_request.identifier)
    job = job_manager.create_job(
        process, wps_request.inputs, wps_request.outputs
    )
    job_manager.execute(job)
    return StatusInfo.from_job(job)


@handles(GetStatusRequest)
async def handle_get_status(process_registry, job_manager, wps_request):
    job = job_manager.get_job(wps_request.job_id)
    return StatusInfo.from_job(job)


@handles(GetResultRequest)
async def handle_get_result(process_registry, job_manager, wps_request):
    pass


@handles(DismissRequest)
async def handle_dismiss(process_registry, job_manager, wps_request):
    pass


async def dispatch(process_registry, job_manager, wps_request):
    return encode_response(
        await REQUEST_HANDLERS[type(wps_request)](
            process_registry, job_manager, wps_request
        )
    )
