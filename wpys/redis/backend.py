

class RedisResult:
    def __init__(self, redis, key):
        self.redis = redis
        self.key = key
        self._closed = False
        self._offset = 0

    async def read(self, size=None):
        if size is None and self._offset == 0:
            data = await self.redis.get(self.key)
        else:
            data = await self.redis.getrange(self._offset, size)
        self._offset += len(data)
        return data

    async def seek(self, offset, from_what=0):
        if from_what == 0:
            self._offset = offset
        elif from_what == 1:
            self._offset += offset
        elif from_what == 2:
            self._offset = await self.size() + offset

    async def size(self):
        return await self.redis.strlen(self.key)


class RedisResultBackend:
    def __init__(self, redis):
        pass

    async def put_job_result(self, job, output_name, result):
        pass

    async def get_job_result(self, job, output_name):
        pass
