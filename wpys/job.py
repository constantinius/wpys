from enum import Enum
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from threading import Thread, RLock, Event
from inspect import signature, Signature, Parameter, isgeneratorfunction, currentframe
from queue import SimpleQueue, Queue
from collections.abc import Iterable
from typing import Callable, Sequence, Any


class JobStatus(Enum):
    ACCEPTED = "Accepted"
    RUNNING = "Running"
    SUCCEEDED = "Succeded"
    FAILED = "Failed"
    PAUSED = "Paused"
    DISMISSED = "Dismissed"

    def __str__(self):
        return self.value


@dataclass
class Job:
    identifier: str
    process: Callable
    status: JobStatus = JobStatus.ACCEPTED
    inputs: Sequence[Any] = ()
    outputs: Sequence[Any] = ()
    results: Sequence[Any] = ()

    errors: Sequence[Any] = ()

    percent_completed: int = None
    estimated_completion: datetime = None
    next_poll: datetime = None

class JobException(Exception):
    pass


class Status:
    percent_completed: int = None


class Result:
    def __init__(self, output):
        self.output = output


class Output:
    def __init__(self, identifier, mimetype, schema=None):
        self.identifier = identifier
        self.mimetype = mimetype
        self.schema = schema

        self.filename = None
        self.file = None
        self.data = None

    def set_filename(self, filename):
        self.filename = filename
        self.file = None
        self.data = None

    def set_file(self, file_):
        self.filename = None
        self.file = file_
        self.data = None

    def set_data(self, data):
        self.filename = None
        self.file = None
        self.data = data


# def _get_job():
#     frame = currentframe()
#     while frame:
#         job = frame.f_locals.get('job')
#         if isinstance(job, Job):
#             return job
#         frame = frame.f_back


# def prepare_output(identifier=None):
#     job = _get_job()
#     process = job.process
#     if not job:
#         raise Exception('No job instance found')

#     if not process.outputs:
#         raise Exception('')

#     if not identifier:
#         if len(process.outputs) != 1:
#             raise Exception('')
#         format_ = process.outputs[0].formats[0]
#         return Output(
#             process.outputs[0].identifier, format_.mimetype, format_.schema
#         )
