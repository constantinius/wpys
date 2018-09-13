from collections.abc import Iterable
import asyncio
from concurrent.futures import ThreadPoolExecutor
import inspect
from functools import partial
import logging
import traceback

from .job import Job, JobStatus, Result, Status

logger = logging.getLogger(__name__)

class CancelledError(Exception):
    pass


class Worker:
    """ Class to work on jobs
    """
    def __init__(self, loop, broker, backend):
        self.loop = loop
        self.broker = broker
        self.backend = backend
        self.executor = ThreadPoolExecutor(1)

    async def run(self):
        """ The main function to iteratively run jobs
        """
        print('Running worker')
        while True:
            # wait for a job
            job = await self.broker.pick_job()
            print(f'Picked job {job.identifier}')

            if not job:
                continue

            # create a task to see if the job shall be cancelled
            cancelled_task = asyncio.ensure_future(
                self.broker.get_job_notification(job.identifier)
            )

            if inspect.isgeneratorfunction(job.process.fn):
                generator = job.process.fn(*job.inputs)
                await self._run_generator(job, generator, cancelled_task)
            elif inspect.isasyncgenfunction(job.process.fn):
                async_generator = job.process.fn(*job.inputs)
                await self._run_async_generator(job, async_generator, cancelled_task)
            elif inspect.iscoroutinefunction(job.process.fn):
                coroutine = job.process.fn(*job.inputs)
                await self._run_coroutine(job, coroutine, cancelled_task)
            elif inspect.isfunction(job.process.fn):
                func = partial(job.process.fn, *job.inputs)
                await self._run_sync(job, func, cancelled_task)
            else:
                # TODO
                raise NotImplementedError

    async def _run_generator(self, job, generator, cancelled_task):
        logger.debug(f'Running job {job.identifier} as generator')
        while True:
            main_task = self.loop.run_in_executor(self.executor, next, generator)
            await asyncio.wait(
                [main_task, cancelled_task], return_when=asyncio.FIRST_COMPLETED
            )

            # detect whether the job was cancelled
            if cancelled_task.done():
                self._handle_job_cancelled(job)
                generator.throw(CancelledError)
                break

            else:
                try:
                    chunk = main_task.result()
                    await self._handle_job_chunk(job, chunk)
                except StopIteration:
                    await self._handle_job_finished(job)
                    break
                except Exception as e:
                    await self._handle_job_exception(job, e)
                    break


    async def _run_async_generator(self, job, async_generator, cancelled_task):
        try:
            async for chunk in async_generator:
                if cancelled_task.done():
                    await self._handle_job_cancelled(job)
                    await async_generator.athrow(CancelledError)
                    break
                await self._handle_job_chunk(job, chunk)
            await self._handle_job_finished(job)
        except Exception as e:
            await self._handle_job_exception(job, e)

    async def _run_coroutine(self, job, coroutine, cancelled_task):
        main_task = asyncio.ensure_future(coroutine)
        await asyncio.wait(
            [main_task, cancelled_task], return_when=asyncio.FIRST_COMPLETED
        )

        if cancelled_task.done():
            await self._handle_job_cancelled(job)
            await main_task.athrow(CancelledError)
        else:
            try:
                chunk = main_task.result()
                await self._handle_job_chunk(job, chunk)
            except Exception as e:
                await self._handle_job_exception(job, e)

    async def _run_sync(self, job, sync, cancelled_task):
        main_task = self.loop.run_in_executor(self.executor, sync)
        await asyncio.wait(
            [main_task, cancelled_task], return_when=asyncio.FIRST_COMPLETED
        )

        if cancelled_task.done():
            await self._handle_job_cancelled(job)

            # wait for the main task to finish
            await main_task
        else:
            try:
                chunk = main_task.result()
                await self._handle_job_chunk(job, chunk)
                await self._handle_job_finished(job)
            except Exception as e:
                await self._handle_job_exception(job, e)

    async def _handle_job_chunk(self, job, chunk):
        logger.debug(f'Handling chunk for job {job.identifier}')
        if not isinstance(chunk, Iterable):
            chunk = [chunk]
        for part in chunk:
            print(f"Chunk {chunk} for job {job}")
            if isinstance(part, Result):
                pass

            elif isinstance(part, Status):
                await self.broker.update_job(job.identifier, part)

    async def _handle_job_exception(self, job, exception):
        logger.error(f'Handling exception for job {job.identifier}')
        logger.exception(exception)
        job.status = JobStatus.FAILED
        job.exception = exception
        job.traceback = traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )
        await self.broker.update_job(job)
    
    async def _handle_job_cancelled(self, job):
        job.status = JobStatus.DISMISSED
        await self.broker.update_job(job)

    async def _handle_job_finished(self, job):
        job.status = JobStatus.SUCCEEDED
        await self.broker.update_job(job)

