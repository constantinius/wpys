from uuid import uuid4
import pickle
from collections.abc import Iterable
import asyncio
import aioredis

from ..job import Job, JobException, JobStatus

JOBS_KEY_TEMPLATE = "jobs:%s"
EXECUTION_QUEUE_KEY = "execute_queue"
JOB_CONTROL_CHANNEL_TEMPLATE = "control:%s"


class RedisBroker:
    """ A broker using redis for data transmission and job control.
    """
    def __init__(self, redis, config):
        self.redis = redis
        self.config = config

    async def create_job(self, job_id, process, inputs, outputs) -> Job:
        """ Create a new Job and persist it in the redis store.
        """
        job = Job(
            identifier=job_id,
            process=process,
            inputs=inputs,
            outputs=outputs,
            results=[],
        )

        # check if an old job with that ID already existed. if yes, 
        # raise an exception
        old_job = await self.get_job(job_id, False)
        if old_job:
            raise JobException(f"Job {job_id} already exists")

        await self.update_job(job)
        return job

    async def get_job(self, job_id, raise_if_not_exist=True) -> str:
        """ Get a registered job from the store. By default, raise an error
            when that job does not exist.
        """
        data = await self.redis.get(JOBS_KEY_TEMPLATE % job_id)
        if not data:
            if raise_if_not_exist:
                raise JobException(f"Job {job_id} does not exist")
            return None
        return pickle.loads(data)

    async def enqueue_job(self, job_id):
        """ Schedule a job for execution, by putting the job ID into the
            execution queue.
        """
        await self.get_job(job_id)
        await self.redis.lpush(EXECUTION_QUEUE_KEY, job_id)

    async def dismiss_job(self, job_id):
        """ Send a signal to dismiss a job: schedule its interruption and cleanup
        """
        # lookup the job, set its status to dismissed and send a notification to
        # cancel the running job
        job = await self.get_job(job_id)
        job.status = JobStatus.DISMISSED
        self.update_job(job)

        channel_name = JOB_CONTROL_CHANNEL_TEMPLATE % job_id
        await self.redis.publish_json(channel_name, ["dismiss"])

    async def update_job(self, job):
        # encode the job using pickle
        encoded = pickle.dumps(job)
        # put the pickled job in the jobs hash
        await self.redis.set(JOBS_KEY_TEMPLATE % job.identifier, encoded)
        if self.config.expiration_time is not None:
            await self.redis.expire(
                JOBS_KEY_TEMPLATE % job.identifier,
                self.config.expiration_time
            )

        print(await self.get_job(job.identifier))

    async def pick_job(self) -> Job:
        """ Wait and pop a job ID from the execution queue, and return a
            job instance.
        """
        job_id = (await self.redis.brpop(EXECUTION_QUEUE_KEY))[1].decode('utf-8')
        print(f"got job id {job_id}")
        if job_id:
            return await self.get_job(job_id)

    async def get_job_notification(self, job_id, messages=None) -> str:
        channel_name = JOB_CONTROL_CHANNEL_TEMPLATE % job_id
        channel = (await self.redis.subscribe(channel_name))[0]
        async for message in channel.iter():
            if not messages or message in messages:
                return message                

    @classmethod
    async def get_broker(cls, config, loop):
        redis = await aioredis.create_redis(
            ('localhost', 6379),
            #config.broker_options.get('127.0.0.1', 'localhost'),
            loop=loop,
        )
        return cls(redis, config)
