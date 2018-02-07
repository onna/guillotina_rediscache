from dateutil.parser import parse
from guillotina.files.adapter import DBDataManager
from guillotina.renderers import GuillotinaJSONEncoder
from guillotina_rediscache import cache

import aioredis
import json
import uuid


class RedisFileDataManager(DBDataManager):

    _redis = None
    _data = None
    _ttl = 60 * 50 * 5  # 5 minutes should be plenty of time between activity

    async def load(self):
        await super().load()
        # preload data
        if self._data is None:
            redis = await self.get_redis()
            key = await self.get_key()
            data = await redis.get(key)
            if not data:
                self._data = {}
            else:
                self._data = json.loads(data)
                if 'resumable_uri_date' in self._data:
                    self._data['resumable_uri_date'] = parse(
                        self._data['resumable_uri_date']
                    )

    async def get_redis(self):
        if self._redis is None:
            conn = await cache.get_redis_pool()
            self._redis = aioredis.Redis(conn)
        return self._redis

    async def get_key(self):
        # only need 1 write to save upload object id...
        if not getattr(self._file, '_redis_upload_id', None):
            self._file._redis_upload_id = '{}-{}'.format(
                self.context._p_oid,
                uuid.uuid4().hex
            )
            await self.save()
        return self._file._redis_upload_id

    async def update(self, **kwargs):
        redis = await self.get_redis()
        key = await self.get_key()
        self._data.update(kwargs)
        await redis.set(key, json.dumps(self._data, cls=GuillotinaJSONEncoder))
        await redis.expire(key, self._ttl)

    async def finish(self):
        # persist the data on db from redis
        await self.load()
        if self._data is not None:
            for key, value in self._data.items():
                setattr(self._file, key, value)
            await self.save()

    @property
    def size(self):
        return self._data.get('size', 0)

    def get_offset(self):
        return self._data.get('current_upload', 0)

    def get(self, name, default=None):
        return self._data.get(name, default)
