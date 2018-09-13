from collections.abc import Iterable
import asyncio
from concurrent.futures import ThreadPoolExecutor
import inspect
import traceback
from functools import partial
import logging
import logging.config

import click

from .config import load_config
from .broker import get_broker
from .backend import get_result_backend
from .worker import Worker

logger = logging.getLogger('wpys.cli')

@click.command()
def main():
    config = load_config()
    if config.logging:
        # logging.basicConfig(level=logging.DEBUG)
        logging.config.dictConfig(config.logging)

    async def amain():
        logger.debug("waiting for broker...")
        broker = await get_broker(config, loop)
        logger.debug("waiting for backend...")
        backend = await get_result_backend(config)
        worker = Worker(loop, broker, backend)
        logger.debug("running worker...")
        await worker.run()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain())


if __name__ == '__main__':
    main()
