from dataclasses import dataclass
from functools import wraps, partial
from typing import List, Tuple, Callable, Union, Any, ClassVar
from inspect import signature, Signature, Parameter, isgeneratorfunction, currentframe
from types import GeneratorType

from weakref import proxy
from threading import Thread, Event, RLock
import weakref
from datetime import datetime, timedelta
from collections.abc import Iterable
from pprint import pprint
from uuid import uuid4

__all__ = ['process']


def process(fn=None, *, identifier=None, inputs=None, outputs=None, allow_async=None,
            allow_sync=None, metadata=None):
    """ Decorator to dynamically a process class from a function definition.
    """
    if fn is None:
        return partial(
            process,
            identifier=identifier,
            inputs=inputs,
            outputs=outputs,
            allow_async=allow_async,
            allow_sync=allow_sync
        )

    sig = signature(fn)

    # build input definition if not already passed
    if not inputs:
        inputs = []
        for param in sig.parameters.values():
            # try to find a suitable default literal value definition
            default = DEFAULT_LITERAL_DATA.get(param.annotation) or DEFAULT_LITERAL_DATA[None]
            inputs.append(default(identifier=param.name))
            if param.default is not Parameter.empty:
                inputs[-1].domains[0].default_value = param.default

    else:
        # some checking
        # TODO: expand?
        if not len(inputs) == len(sig.parameters):
            raise Exception('Invalid number of inputs specified.')

    if not outputs:
        outputs = []
        # TODO: allow multiple outputs when a tuple is used
        if sig.return_annotation is not Signature.empty:
            default = DEFAULT_LITERAL_DATA.get(sig.return_annotation) or DEFAULT_LITERAL_DATA[None]
            outputs.append(default(identifier='output'))

    else:
        # TODO: convert to LiteralData types
        outputs = [
            output if isinstance(output, LiteralData) else LiteralData(
                identifier=output["identifier"],
                title=output.get("title"),
                abstract=output.get("abstract"),
                keywords=output.get("keywords"),
                formats=[
                    format_ if isinstance(format_, Format) else Format(**format_)
                    for format_ in output.get("formats", [])
                ],
                metadatas=[
                    metadata if isinstance(metadata, Metadata) else Metadata(**metadata)
                    for metadata in output.get("metadatas", [])
                ],
                domains=[
                    domain if isinstance(domain, Domain) else Domain(**domain)
                    for domain in output.get("domains", [])
                ],
            )
            for output in outputs
        ]


    if metadata is not None and not isinstance(metadata, Metadata):
        metadata = Metadata(
            references=[
                MetadataReference(**reference)
                for reference in metadata.pop('references')
            ], **metadata
        )
    elif not metadata:
        metadata = Metadata()

    return ProcessWrapper(
        fn=fn,
        identifier=identifier or fn.__name__,
        inputs=inputs,
        outputs=outputs,
        allow_async=allow_async,
        allow_sync=allow_sync,
        metadata=metadata
    )


@dataclass
class MetadataReference:
    href: str
    title: str = None
    role: str = None


@dataclass
class Metadata:
    title: str = ''
    abstract: str = ''
    keywords: List[str] = None
    references: List[MetadataReference] = None


@dataclass
class Format:
    mimetype: str
    encoding: str = None
    schema: str = None
    maximum_megabytes: int = None
    value_parser: Callable[[str], Any] = None


# http://www.w3.org/2001/XMLSchema#string
# http://www.w3.org/2001/XMLSchema#integer
# http://www.w3.org/2001/XMLSchema#decimal
# http://www.w3.org/2001/XMLSchema#boolean
# http://www.w3.org/2001/XMLSchema#double
# http://www.w3.org/2001/XMLSchema#float


class DataDescription:
    identifier: str
    title: str = None
    abstract: str = None
    keywords: List[str]
    metadatas: List[Metadata]
    formats: List[Format]


@dataclass
class Domain:
    data_type: str
    allowed_values: List[Union[float, int]] = None
    uom: str = None
    default_value: Union[float, int] = None
    to_default_domain: Callable[[Any], Any] = None

@dataclass
class LiteralData(DataDescription):
    identifier: str
    title: str = None
    abstract: str = None
    keywords: List[str] = None
    metadatas: List[Metadata] = None
    formats: List[Format] = None
    domains: List[Domain] = None
    value_parser: Callable[[str], Any] = None

@dataclass
class BoundingBoxData:
    identifier: str
    supported_crss: List[str]
    title: str = None
    abstract: str = None
    keywords: List[str] = None
    metadatas: List[Metadata] = None
    formats: List[Format] = None

@dataclass
class ComplexData:
    identifier: str
    title: str = None
    abstract: str = None
    keywords: List[str] = None
    metadatas: List[Metadata] = None
    formats: List[Format] = None

def parse_bool(value):
    lower = value.lower()
    if lower == "true":
        return True
    elif lower == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean value '{value}'")

DEFAULT_LITERAL_DATA = {
    str: partial(LiteralData,
        formats=[Format(mimetype='text/plain'), Format(mimetype='text/xml')],
        domains=[Domain(data_type='http://www.w3.org/2001/XMLSchema#string')],
    ),
    int: partial(LiteralData,
        value_parser=int,
        formats=[Format(mimetype='text/plain'), Format(mimetype='text/xml')],
        domains=[Domain(data_type='http://www.w3.org/2001/XMLSchema#integer')],
    ),
    float: partial(LiteralData,
        value_parser=float,
        formats=[Format(mimetype='text/plain'), Format(mimetype='text/xml')],
        domains=[Domain(data_type='http://www.w3.org/2001/XMLSchema#double')],
    ),
    bool: partial(LiteralData,
        value_parser=parse_bool,
        formats=[Format(mimetype='text/plain'), Format(mimetype='text/xml')],
        domains=[Domain(data_type='http://www.w3.org/2001/XMLSchema#boolean')],
    ),
    None: partial(LiteralData,
        formats=[Format(mimetype='text/plain'), Format(mimetype='text/xml')],
        domains=[Domain(data_type='http://www.w3.org/2001/XMLSchema#string')],
    ),
}


@dataclass(init=False)
class ProcessWrapper:
    fn: Callable[..., bool]
    identifier: str
    inputs: List[str]
    outputs: List[str]
    allow_async: bool
    allow_sync: bool
    metadata: Metadata

    def __init__(self, fn, identifier, inputs, outputs, allow_async, allow_sync, metadata=None):
        self.fn = fn
        self.identifier = identifier
        self.inputs = inputs
        self.outputs = outputs
        self.allow_async = allow_async
        self.allow_sync = allow_sync
        self.metadata = metadata

        self.__call__ = fn

    def parse_input(self, input_):
        identifier = input_.identifier
        for input_def in self.inputs:
            if input_def.identifier == identifier:
                break
        else:
            raise Exception(f"No such input {identifier}")

        data = input_.data

        def parse_literal_data(data):
            parts = data.split('@')
            value = parts[0]
            args = dict(
                item.partition('=')[::2]
                for item in parts[1:]
            )
            return value, args

        if isinstance(input_def, LiteralData):
            raw_value, args = parse_literal_data(data.data)
            if data.mimetype:
                for format_ in input_def.formats:
                    if data.mimetype == format_.mimetype:
                        break
                else:
                    raise Exception(f"Invalid format {format_.mimetype}")
            else:
                format_ = input_def.formats[0]

            value_parser = (format_.value_parser or input_def.value_parser)
            if value_parser:
                value = value_parser(raw_value)
            else:
                value = raw_value

            uom = args.get('uom')
            if uom:
                for domain in input_def.domains:
                    if domain.uom == uom:
                        break
                    else:
                        raise Exception(f"Unsupported UOM {uom}")
            else:
                domain = input_def.domains[0]

            if domain is not input_def.domains[0]:
                # TODO: convert from other domain to default domain
                domain.to_default_domain(value)
            
            return value

        elif isinstance(input_def, BoundingBoxData):
            # TODO: implement
            raise NotImplementedError
        elif isinstance(input_def, ComplexData):
            # TODO: implement
            raise NotImplementedError


# @process
# def example_process():
#     pass

# print(example_process)


# @process(identifier='Some:ID')
# def example_process2():
#     pass

# print(example_process2)


@process(identifier='Some:ID')
def example_process3(a: int, b: int) -> int:
    pass


# print(example_process3)

# print(type(req.identifiers[0]))

# from tqdm import tqdm

# def main():
#     registry = ProcessRegistry()
#     registry.register(long_running_process)

#     manager = JobManager()

#     job = registry.create_job('long_running_process', [2], [])

#     with manager:
#         manager.execute(job)
#         with tqdm(total=100) as bar:
#             try:
#                 while job.status in (JobStatus.ACCEPTED, JobStatus.RUNNING):
#                     if job.status_info.percent_completed:
#                         # print(job.status_info)
#                         bar.update(job.status_info.percent_completed)
#                     if job.status_info.percent_completed == 100:
#                         break
#                     time.sleep(0.1)
#             except KeyboardInterrupt:
#                 job.interrupt()
#                 print(job.is_interrupted)

# main()

