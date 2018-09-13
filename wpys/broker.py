from asyncio import AbstractEventLoop

from .config import WPySConfig
from .redis.broker import RedisBroker

BROKER = None

async def get_broker(config: WPySConfig, loop: AbstractEventLoop):
    global BROKER

    if BROKER is None:
        if config.broker_type == "redis":
            BROKER = await RedisBroker.get_broker(
                config, loop
            )
        # TODO: other broker types

    return BROKER
