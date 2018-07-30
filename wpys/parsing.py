from dataclasses import dataclass
from typing import List, Tuple, Callable, Union, Any, ClassVar
from datetime import datetime, timedelta
from lxml import etree

nsmap = {
    "wps": "http://www.opengis.net/wps/2.0",
    "ows": "http://www.opengis.net/ows/2.0",
    "xlink": "http://www.w3.org/1999/xlink",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}


class Request:
    service: str = "WPS"
    version: str = "2.0.0"
    request: str = None


@dataclass(frozen=True)
class GetCapabilitiesRequest(Request):
    request: ClassVar = "GetCapabilities"
    sections: List[str] = None


@dataclass(frozen=True)
class DescribeProcessRequest(Request):
    request: ClassVar = "DescribeProcess"
    identifiers: List[str]

    @classmethod
    def from_node(cls, root):
        return cls(identifiers=[str(v) for v in root.xpath('ows:Identifier/text()', namespaces=nsmap)])

    @classmethod
    def from_kvp(cls, kvp):
        return cls(identifiers=kvp['identifier'].split(','))  

@dataclass
class Reference:
    href: str = None
    request_body: str = None
    mimetype: str = None
    schema: str = None
    encoding: str = None

@dataclass
class Data:
    data: Any
    mimetype: str = None
    schema: str = None
    encoding: str = None

@dataclass
class Input:
    identifier: str
    data: Data = None
    reference: Reference = None

    @classmethod
    def from_node(cls, node):
        data = None
        reference = None
        data_node = next(iter(node.xpath('wps:Data', namespaces=nsmap)), None)
        reference_node = next(iter(node.xpath('wps:Reference', namespaces=nsmap)), None)
        if reference_node is not None:
            reference = Reference(
                mimetype=node.attrib.get('mimetype'),
                schema=node.attrib.get('schema'),
                encoding=node.attrib.get('encoding'),
                href=reference_node.attrib.get(f"{{{nsmap['xlink']}}}href"),
            )
        elif data_node is not None:
            data = Data(
                mimetype=node.attrib.get('mimetype'),
                schema=node.attrib.get('schema'),
                encoding=node.attrib.get('encoding'),
                data=data_node.text
            )
        return cls(
            identifier=node.attrib['id'],
            reference=reference,
            data=data,
        )

@dataclass
class Output:
    identifier: str
    transmission: str = None
    mimetype: str = None
    schema: str = None
    encoding: str = None

    @classmethod
    def from_node(cls, node):
        return cls(
            identifier=node.attrib['id'],
            transmission=node.attrib.get(
                f"{{{nsmap['wps']}}}dataTransmissionMode"
            ),
            mimetype=node.attrib.get('mimetype'),
            schema=node.attrib.get('schema'),
            encoding=node.attrib.get('encoding'),
        )

@dataclass(frozen=True)
class ExecuteRequest(Request):
    request: ClassVar = "Execute"
    identifier: str
    inputs: List[Any]
    outputs: List[Any]
    response: str = "document"
    mode: str = "async"

    @classmethod
    def from_node(cls, root):
        return cls(
            identifier=str(
                root.xpath('ows:Identifier/text()', namespaces=nsmap)[0].strip()
            ),
            inputs=[
                Input.from_node(node)
                for node in root.xpath('wps:Input', namespaces=nsmap)
            ],
            outputs=[
                Output.from_node(node)
                for node in root.xpath('wps:Output', namespaces=nsmap)
            ],
            response=root.attrib["response"],
            mode=root.attrib["mode"],
        )

    @classmethod
    def from_kvp(cls, kvp):
        raise NotImplementedError
        # TODO?
        # return cls(job_id=kvp['jobid'])


class JobRelatedRequestMixIn:
    @classmethod
    def from_node(cls, root):
        return cls(job_id=str(root.xpath('wps:JobID/text()', namespaces=nsmap)[0]))

    @classmethod
    def from_kvp(cls, kvp):
        return cls(job_id=kvp['jobid'])


@dataclass(frozen=True)
class GetStatusRequest(JobRelatedRequestMixIn, Request):
    request: ClassVar = "GetStatus"
    job_id: str


@dataclass(frozen=True)
class GetResultRequest(JobRelatedRequestMixIn, Request):
    request: ClassVar = "GetResult"
    job_id: str


@dataclass(frozen=True)
class DismissRequest(JobRelatedRequestMixIn, Request):
    request: ClassVar = "Dismiss"
    job_id: str


@dataclass(frozen=True)
class PauseRequest(JobRelatedRequestMixIn, Request):
    request: ClassVar = "Pause"
    job_id: str


@dataclass(frozen=True)
class ResumeRequest(JobRelatedRequestMixIn, Request):
    request: ClassVar = "Resume"
    job_id: str


REQUEST_CLASSES = {
    cls.request.upper(): cls
    for cls in [
        GetCapabilitiesRequest,
        DescribeProcessRequest,
        ExecuteRequest,
        GetStatusRequest,
        GetResultRequest,
        DismissRequest,
        PauseRequest,
        ResumeRequest,
    ]
}


def parse_xml_request(request: str):
    root = etree.fromstring(request)
    namespace, _, tagname = root.tag.partition('}')
    namespace = namespace[1:]
    if namespace not in (nsmap["ows"], nsmap["wps"]):
        raise Exception('wrong request')
    cls = REQUEST_CLASSES.get(tagname.upper())
    if not cls:
        raise Exception(f"Invalid request {tagname}")

    return cls.from_node(root)


def parse_kvp_request(kvp: dict):
    try:
        if kvp['service'].upper() != "WPS":
            raise Exception(f"Invalid service {kvp['service']}")

        if kvp['version'] != "2.0.0":
            raise Exception(f"Invalid version {kvp['version']}")

        cls = REQUEST_CLASSES[kvp['request'].upper()]
        if not cls:
            raise Exception(f"Invalid request {kvp['request']}")
    except KeyError as e:
        raise Exception(f"Missing mandatory key {e}")

    return cls.from_kvp(kvp)


# req = parse_xml_request("""
# <wps:DescribeProcess service="WPS" version="2.0.0"
#   xmlns:ows="http://www.opengis.net/ows/2.0"
#   xmlns:wps="http://www.opengis.net/wps/2.0"
#   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
#   xsi:schemaLocation="http://www.opengis.net/wps/2.0 ../wps.xsd">
#   <ows:Identifier>Buffer</ows:Identifier>
#   <ows:Identifier>Viewshed</ows:Identifier>
# </wps:DescribeProcess>
# """)

# req = parse_xml_request("""
# <wps:Execute
#   xmlns:wps="http://www.opengis.net/wps/2.0"
#   xmlns:ows="http://www.opengis.net/ows/2.0"
#   xmlns:xlink="http://www.w3.org/1999/xlink"
#   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
#   xsi:schemaLocation="http://www.opengis.net/wps/2.0 ../wps.xsd"
#   service="WPS" version="2.0.0" response="document" mode="async">
#   <ows:Identifier>
#     http://my.wps.server/processes/proximity/Planar-Buffer
#   </ows:Identifier>
#   <wps:Input id="INPUT_GEOMETRY">
#     <wps:Reference xlink:href="http://some.data.server/mygmldata.xml"/>
#   </wps:Input>
#   <wps:Input id="DISTANCE">
#     <wps:Data>10</wps:Data>
#   </wps:Input>
#   <!-- Uses default output format -->
#   <wps:Output id="BUFFERED_GEOMETRY" wps:dataTransmissionMode="reference" />
# </wps:Execute>
# """)


# req = parse_xml_request("""
# <wps:Execute
#   xmlns:wps="http://www.opengis.net/wps/2.0"
#   xmlns:ows="http://www.opengis.net/ows/2.0"
#   xmlns:xlink="http://www.w3.org/1999/xlink"
#   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
#   xsi:schemaLocation="http://www.opengis.net/wps/2.0 ../wps.xsd"
#   service="WPS" version="2.0.0" response="document" mode="async">
#   <ows:Identifier>long_running_process</ows:Identifier>
#   <wps:Input id="sleep_time">
#     <wps:Data>10@uom=seconds</wps:Data>
#   </wps:Input>
#   <!-- Uses default output format -->
#   <wps:Output id="BUFFERED_GEOMETRY" wps:dataTransmissionMode="reference" />
# </wps:Execute>
# """)


# print(req)


# from .process import ProcessRegistry, JobManager, long_running_process


# from tqdm import tqdm

# def main():
#     registry = ProcessRegistry()
#     registry.register(long_running_process)

#     # manager = JobManager()

#     for inp in req.inputs:
#         long_running_process.parse_input(inp)

#     # job = registry.create_job('long_running_process', [2], [])

#     # with manager:
#     #     manager.execute(job)
#     #     with tqdm(total=100) as bar:
#     #         try:
#     #             while job.status in (JobStatus.ACCEPTED, JobStatus.RUNNING):
#     #                 if job.status_info.percent_completed:
#     #                     # print(job.status_info)
#     #                     bar.update(job.status_info.percent_completed)
#     #                 if job.status_info.percent_completed == 100:
#     #                     break
#     #                 time.sleep(0.1)
#     #         except KeyboardInterrupt:
#     #             job.interrupt()
#     #             print(job.is_interrupted)

# main()
