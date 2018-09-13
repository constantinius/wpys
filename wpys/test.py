
from concurrent.futures import ThreadPoolExecutor
import asyncio
from time import sleep
from functools import partial


loop = asyncio.get_event_loop()


def func(seconds):
    print("start")
    sleep(seconds)
    print("stop")


async def main():
    executor = ThreadPoolExecutor(1)
    main_task = loop.run_in_executor(executor, func, 1)
    timeout_task = asyncio.sleep(0.5)
    done, pending = await asyncio.wait(
        [main_task, timeout_task], return_when=asyncio.FIRST_COMPLETED
    )

    print(done, pending)


if __name__ == "__main__":
   loop.run_until_complete(main())
