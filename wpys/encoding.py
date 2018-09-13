from dataclasses import dataclass
from typing import List, Tuple, Callable, Union, Any, ClassVar
from datetime import datetime
from traceback import format_tb, format_exception
from functools import partial

from lxml import etree
from lxml.builder import ElementMaker

from .process import LiteralData, ComplexData, BoundingBoxData
from .config import WPySConfig


class BetterElementMaker(ElementMaker):
    def __call__(self, *args, **kwargs):
        """ Better version of the ElementMaker, allows `None` in
            __call__ (which is filtered out).
        """
        args = [
            arg
            for arg in args
            if arg is not None
        ]
        kwargs = {
            key: value
            for key, value in kwargs.items()
            if value is not None
        }
        return super().__call__(*args, **kwargs)

nsmap = {
    "wps": "http://www.opengis.net/wps/2.0",
    "ows": "http://www.opengis.net/ows/2.0",
    "xlink": "http://www.w3.org/1999/xlink",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

SCHEMA_LOCATION = {f"{{{nsmap['xsi']}}}schemaLocation": "http://www.opengis.net/wps/2.0 ../wps.xsd"}

OWS = BetterElementMaker(namespace=nsmap['ows'], nsmap=nsmap)
WPS = BetterElementMaker(namespace=nsmap['wps'], nsmap=nsmap)

def xlink(key, value):
    return {f"{{{nsmap['xlink']}}}{key}": value}

class Response:
    def encode_xml(self):
        pass

@dataclass
class Operation:
    name: str
    url: str
    post_url: str = None
    get_enabled: bool = True
    post_enabled: bool = True

    def encode_tree(self):
        urls = []
        if self.get_enabled:
            urls.append(OWS("Get", xlink("href", self.url)))
        if self.post_enabled:
            urls.append(OWS("Post", xlink("href", self.post_url or self.url)))
        return OWS("Operation",
            OWS("DCP",
                OWS("HTTP", *urls)
            ),
            name=self.name,
        )

DEFAULT_OPERATIONS = (
    partial(Operation, name="GetCapabilities"),
    partial(Operation, name="DescribeProcess"),
    partial(Operation, name="Execute", get_enabled=False),
    partial(Operation, name="GetStatus"),
    partial(Operation, name="GetResult"),
    partial(Operation, name="Dismiss"),
)

@dataclass
class Capabilities(Response):
    title: str = None
    abstract: str = None
    keywords: List[str] = None
    fees: str = None
    access_constraints: str = None

    provider_name: str = None
    provider_site: str = None
    individual_name: str = None
    electronical_mail_address: str = None

    service_endpoint: str = '.'

    processes: List[Any] = ()
    operations: List[Operation] = DEFAULT_OPERATIONS

    def encode_tree(self):
        return OWS("Capabilities",
            OWS("ServiceIdentification",
                OWS("Title", self.title) if self.title else None,
                OWS("Abstract", self.abstract) if self.abstract else None,
                OWS("Keywords", *[
                    OWS("Keyword", keyword)
                    for keyword in self.keywords or []
                ]) if self.keywords else None,
                OWS("ServiceType", "WPS"),
                OWS("ServiceVersion", "2.0.0"),
                OWS("Fees", self.fees) if self.fees else None,
                OWS("AccessConstraints",
                    self.access_constraints
                ) if self.access_constraints else None,
            ),
            OWS("ServiceProvider",
                OWS("ProviderName", self.provider_name) if self.provider_name else None,
                OWS("ProviderSite",
                    xlink("href", self.provider_site)
                ) if self.provider_site else None,
                OWS("ServiceContact",
                    OWS("IndividualName",
                        self.individual_name
                    ) if self.individual_name else None,
                    OWS("ContactInfo",
                        OWS("Address",
                            OWS("ElectronicalMailAddress",
                                self.electronical_mail_address,
                            ) if self.electronical_mail_address else None,
                        ),
                    ),
                ),
            ),
            OWS("OperationsMetadata", *[
                operation(url=self.service_endpoint).encode_tree()
                for operation in self.operations
            ]),
            WPS("Contents", *[
                WPS("Process",
                    OWS("Title", process.metadata.title) if process.metadata.title else None,
                    OWS("Abstract", process.metadata.abstract) if process.metadata.abstract else None,
                    OWS("Keywords", *[
                        OWS("Keyword", keyword)
                        for keyword in process.metadata.keywords or []
                    ]) if process.metadata.keywords else None,
                    OWS("Identifier", process.identifier), *[
                        OWS("Metadata", reference)
                        for reference in process.metadata.references or []
                    ]
                )
                for process in self.processes
            ]),
            SCHEMA_LOCATION
        )

@dataclass
class ProcessOfferings:
    processes: List[Any]

    def encode_formats(self, formats):
        return [
            WPS("Format",
                mimeType=frmt.mimetype,
                encoding=frmt.encoding,
                schema=frmt.schema,
                maximumMegabytes=frmt.maximum_megabytes,
            )
            for i, frmt in enumerate(formats)
        ]


    def encode_data(self, data):
        if isinstance(data, LiteralData):
            return WPS("LiteralData", *self.encode_formats(data.formats) + [
                WPS("LiteralDataDomain",
                    OWS("AllowedValues", 
                        OWS("Range",
                            OWS("MinimumValue", str(domain.allowed_values[0])),
                            OWS("MaximumValue", str(domain.allowed_values[1])),
                        )
                    ) if domain.allowed_values else None,
                    OWS("DataType",
                        {f"{{{nsmap['ows']}}}reference": domain.data_type}
                    ),
                    OWS("UOM", domain.uom) if domain.uom else None,
                    OWS("DefaultValue",
                        str(domain.default_value)
                    ) if domain.default_value is not None else None,
                    default="true" if i == 0 else None
                )
                for i, domain in enumerate(data.domains)
            ])
        elif isinstance(data, ComplexData):
            return WPS("ComplexData", *self.encode_formats(data.formats))

        elif isinstance(data, BoundingBoxData):
            return WPS("BoundingBoxData", *self.encode_formats(data.formats) + [
                WPS("SupportedCRS",
                    suppported_crs,
                    default="true" if i == 0 else None
                )
                for i, suppported_crs in enumerate(data.supported_crss)
            ])

    def encode_input(self, input_):
        return WPS("Input",
            OWS("Title", input_.title) if input_.title else None,
            OWS("Abstract", input_.abstract) if input_.abstract else None,
            OWS("Keywords", *[
                OWS("Keyword", keyword)
                for keyword in input_.keywords or []
            ]) if input_.keywords else None,
            OWS("Identifier", input_.identifier), *[
                OWS("Metadata", reference)
                for reference in input_.metadatas or []
            ] + [
                self.encode_data(input_)
            ]
        )

    def encode_output(self, output):
        return WPS("Output",
            OWS("Title", output.title) if output.title else None,
            OWS("Abstract", output.abstract) if output.abstract else None,
            OWS("Keywords", *[
                OWS("Keyword", keyword)
                for keyword in output.keywords or []
            ]) if output.keywords else None,
            OWS("Identifier", output.identifier), *[
                OWS("Metadata", reference)
                for reference in output.metadatas or []
            ] + [
                self.encode_data(output)
            ]
        )

    def encode_tree(self):
        return WPS("ProcessOfferings", *[
            WPS("ProcessOffering", 
                WPS("Process",
                    OWS("Title", process.metadata.title) if process.metadata.title else None,
                    OWS("Abstract", process.metadata.abstract) if process.metadata.abstract else None,
                    OWS("Keywords", *[
                        OWS("Keyword", keyword)
                        for keyword in process.metadata.keywords or []
                    ]) if process.metadata.keywords else None,
                    OWS("Identifier", process.identifier), *[
                        OWS("Metadata", reference)
                        for reference in process.metadata.references or []
                    ] + [
                        self.encode_input(input_)
                        for input_ in process.inputs
                    ] + [
                        self.encode_output(output)
                        for output in process.outputs
                    ]
                ),
                jobControlOptions="sync-execute async-execute dismiss",
                outputTransmission="value reference",
            )
            for process in self.processes
        ], SCHEMA_LOCATION)

@dataclass
class StatusInfo:
    job_id: str
    status: str
    expiration_date: datetime = None
    next_poll: datetime = None
    estimated_completion: datetime = None
    percent_completed: int = None

    traceback: Any = None

    @classmethod
    def from_job(cls, job):
        # info = job.get_status_info()
        return cls(
            job_id=job.identifier,
            status=str(job.status),
            next_poll=job.next_poll,
            estimated_completion=job.estimated_completion,
            percent_completed=job.percent_completed,
            # traceback=job.error.__traceback__ if job.error else None
        )

    def encode_tree(self):
        return WPS("StatusInfo",
            WPS("JobID", self.job_id),
            WPS("Status", self.status),
            WPS(
                "ExpirationDate", self.expiration_date.isoformat("T")
            ) if self.expiration_date else None,
            WPS(
                "NextPoll", self.next_poll.isoformat("T")
            ) if self.next_poll else None,
            WPS(
                "EstimatedCompletion", self.estimated_completion.isoformat("T")
            ) if self.estimated_completion else None,
            WPS(
                "PercentCompleted", str(self.percent_completed)
            ) if self.percent_completed is not None else None,
            # etree.Comment(
            #     "\n".join(format_tb(self.traceback))
            # ) if self.traceback else None
        )

@dataclass
class Result:

    @classmethod
    def from_job(cls, job):
        pass

    def encode_tree(self):
        pass

@dataclass
class ExceptionReport:
    exceptions: List[Exception]
    debug: bool = False

    @classmethod
    def from_job(cls, job, config=None):
        pass

    @classmethod
    def from_exception(cls, exc, config=None):
        debug = config.debug if config and config.debug else False
        return cls(exceptions=[exc], debug=debug)

    def encode_exception(self, exception):
        locator = None
        traceback = None
        if self.debug:
            locator = format_tb(exception.__traceback__)[-2]
            traceback = etree.Comment(
                '\n' + '\n'.join(format_exception(
                    type(exception), exception, exception.__traceback__
                ))
            )
        return OWS("Exception",
            OWS("ExceptionText", str(exception)),
            traceback,
            exceptionCode=type(exception).__name__,
            locator=locator,
        )

    def encode_tree(self):
        return OWS("ExceptionReport", *[
            self.encode_exception(exception)
            for exception in self.exceptions
        ])

def encode_response(response: Response, config: WPySConfig):
    return etree.tostring(
        response.encode_tree(), pretty_print=config.pretty_print
    )
