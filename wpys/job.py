from uuid import uuid4
from enum import Enum
from datetime import datetime, timedelta
from dataclasses import dataclass
from threading import Thread, RLock, Event
from inspect import signature, Signature, Parameter, isgeneratorfunction, currentframe
from queue import SimpleQueue, Queue
from collections.abc import Iterable


@dataclass(init=False)
class Status:
    percent_completed: int = None
    estimated_completion: datetime = None
    next_poll: datetime = None

    def __init__(self, percent_completed=None, estimated_completion=None, next_poll=None):
        self.percent_completed = percent_completed

        # convert to datetime
        if isinstance(estimated_completion, timedelta):
            estimated_completion = datetime.now() + estimated_completion
        if isinstance(next_poll, timedelta):
            next_poll = datetime.now() + next_poll

        self.estimated_completion = estimated_completion
        self.next_poll = next_poll


class JobStatus(Enum):
    ACCEPTED = "Accepted"
    RUNNING = "Running"
    SUCCEEDED = "Succeded"
    FAILED = "Failed"
    PAUSED = "Paused"
    DISMISSED = "Dismissed"

    def __str__(self):
        return self.value


class Job:
    def __init__(self, identifier, process, inputs, outputs, status=JobStatus.ACCEPTED):
        self.identifier = identifier
        self.process = process
        self.inputs = inputs
        self.outputs = outputs

        self.status = status

        self.results = {}
        self.error = None

        self.runner = None

        self.status_info = Status()

        self.lock = RLock()
        self.interrupt_event = Event()

    # @property
    # def status(self):
    #     with self.lock:
    #         return self._status
    
    # @status.setter
    # def set_status(self, value):
    #     with self.lock:
    #         self._status = value

    def __iter__(self):
        if isgeneratorfunction(self.process.fn):
            return self.process.fn(*self.inputs)
        else:
            return [self.process.fn(*self.inputs)]

    def get_status_info(self):
        with self.lock:
            return self.status_info

    def set_status_info(self, status_info):
        with self.lock:
            self.status_info = status_info

    def set_error(self, error):
        with self.lock:
            self.error = error

    def set_result(self, result):
        output = result.output
        if not isinstance(output, Output):
            index = len(self.results)
            try:
                identifier = self.process.outputs[index].identifier
            except IndexError:
                raise Exception(f'Invalid output index: {index}')
            self.results[identifier] = result
        else:
            # TODO: assure that output exists?
            self.results[output.identifier] = result

    def get_result(self, identifier_or_index=0):
        if isinstance(identifier_or_index, int):
            identifier = self.process.outputs[identifier_or_index].identifier
            return self.results.get(identifier)
        else:
            return self.results.get(identifier_or_index)

    def interrupt(self):
        self.interrupt_event.set()

    def clear_interrupt(self):
        self.interrupt_event.clear()

    @property
    def is_interrupted(self):
        return self.interrupt_event.is_set()


class JobManager:
    def __init__(self, numworkers=4):
        self.active_jobs = {}
        self.all_jobs = {}
        self.job_queue = Queue()
        self.done_queue = SimpleQueue()
        self.workers = [
            JobWorker(self.job_queue, self.done_queue)
            for _ in range(numworkers)
        ]
        for worker in self.workers:
            worker.start()

        self.cleaner_worker = Thread(target=self._job_cleaner)
        self.cleaner_worker.start()

    def create_job(self, process, inputs, outputs):
        return Job(str(uuid4()), process, [
            process.parse_input(input_)
            for input_ in inputs
        ], outputs)

    def execute(self, job):
        """ Execute the provided job: register it and put it in the
            job queue for processing in the workers.
        """
        identifier = job.identifier
        if identifier in self.active_jobs:
            raise Exception(f"Job {identifier} is already registered.")
        self.active_jobs[identifier] = job
        self.all_jobs[identifier] = job
        self.job_queue.put(job)

    def get_job(self, job_id):
        try:
            return self.all_jobs[job_id]
        except KeyError:
            raise Exception(f"No such job {job_id}")

    def pause(self, job_id):
        """ Interrupt the referenced job. It can later be resumed.
        """
        job = self.get_job(job_id)
        job.status = JobStatus.ACCEPTED
        job.interrupt()

    def resume(self, job_id):
        """
        """
        job = self.get_job(job_id)
        job.status = JobStatus.ACCEPTED
        job.clear_interrupt()
        self.job_queue.put(job)

    def dismiss(self, job_id):
        """
        """
        job = self.get_job(job_id)
        job.status = JobStatus.DISMISSED
        job.interrupt()
        del self.active_jobs[job_id]

    def _job_cleaner(self):
        while True:
            job = self.done_queue.get()
            if job is None:
                break

            if job.error:
                job.status = JobStatus.FAILED
            else:
                job.status = JobStatus.SUCCEEDED

            print(f"cleaning job {job.identifier}")
            del self.active_jobs[job.identifier]

    def shutdown(self):
        self.job_queue.join()
        for _ in range(len(self.workers)):
            self.job_queue.put(None)
        self.done_queue.put(None)
        for worker in self.workers:
            worker.join()

        self.cleaner_worker.join()

    # manager protocol

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()


class JobWorker(Thread):
    """ The worker thread class to work on jobs.
    """

    def __init__(self, job_queue, done_queue):
        super().__init__()
        self.job_queue = job_queue
        self.done_queue = done_queue

    def run(self):
        while True:
            # get the next job from the queue
            job = self.job_queue.get()

            # exit condition
            if job is None:
                break
            print(job)
            job.status = JobStatus.RUNNING
            print(f"Working on job {job}")

            try:
                for chunk in job:
                    if job.is_interrupted:
                        print(f"Interrupting job {job}")
                        break

                    if not isinstance(chunk, Iterable):
                        chunk = [chunk]

                    for part in chunk:
                        print(f"Chunk {chunk} for job {job}")
                        if isinstance(part, Result):
                            pass

                        elif isinstance(part, Status):
                            job.status_info = part

            except Exception as e:
                print(f"Error for job {job}: {e}")
                job.set_error(e)

            finally:
                print(f"Finally {job}")
                self.job_queue.task_done()
                self.done_queue.put(job)
                print(f"Done with {job}")


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


def _get_job():
    frame = currentframe()
    while frame:
        job = frame.f_locals.get('job')
        if isinstance(job, Job):
            return job
        frame = frame.f_back


def prepare_output(identifier=None):
    job = _get_job()
    process = job.process
    if not job:
        raise Exception('No job instance found')

    if not process.outputs:
        raise Exception('')

    if not identifier:
        if len(process.outputs) != 1:
            raise Exception('')
        format_ = process.outputs[0].formats[0]
        return Output(
            process.outputs[0].identifier, format_.mimetype, format_.schema
        )
