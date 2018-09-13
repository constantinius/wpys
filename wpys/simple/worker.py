from threading import Thread
from ..worker import WorkerBase


class JobWorker(WorkerBase, Thread):
    """ The worker thread class to work on jobs.
    """

    def __init__(self, job_queue, done_queue):
        super().__init__()
        self.job_queue = job_queue
        self.done_queue = done_queue

    def wait_for_job(self):
        return self.job_queue.get()

    def test_exit(self, job):
        """ Test whether the exit condition is met.
        """
        if job is None:
            return True
        return False

    def update_job_status(self, job, status=None, info=None):
        """ Update the Jobs status and status information
        """
        if status is not None:
            job.status = status
        if info is not None:
            job.status_info = info

    def add_job_result(self, job, output):
        pass

    def handle_job_error(self, job, error):
        """ Handle an encountered error in some way if necessary.
            The status does not have to be set to "Failed".
        """
        job.set_error(error)

    def finalize_job(self, job):
        """ Finalize a finished, failed, or dismissed job if necessary.
        """
        self.job_queue.task_done()
        self.done_queue.put(job)
