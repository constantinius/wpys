
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
