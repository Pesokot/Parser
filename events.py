import asyncio
import time
import orjson
from contextlib import asynccontextmanager

import logging
logger = logging.getLogger('event_publisher')

# timeout=5
# можно в транзакциб положить expire если надо.

try:
    import aioredis
    logger.warning(f"using {aioredis.VERSION=}")
except:
    from redis import asyncio as aioredis
    logger.warning(f"using redis.asyncio!")


class EventPublisherPool():
    @classmethod
    @asynccontextmanager
    async def create(cls, redis_list, bookmaker_id, publisher='', *, use_transactions=True):
        self = EventPublisherPool()

        self.channel = f"events:{bookmaker_id}"
        self.hash_path = f"match:{bookmaker_id}"
        self.use_transactions = use_transactions

        self.redises = [ aioredis.from_url( f"redis://{host}:{port}", password=password) for host, port, password in redis_list ]

        try:
            await self.send("parser_started")
            yield self
        finally:
            await self.send("parser_finished")
            for redis in self.redises:
                await redis.close()

    async def send(self, code, data=None, *, msg_ts=None):
        jmsg, jdata = build_message(code, data, msg_ts)

        tasks = [ self._transact(redis, code, jmsg, jdata, data) for redis in self.redises ]
        await asyncio.gather(*tasks)
        logger.debug(f"event sent: {jmsg}")

    async def set(self, key, value):
        tasks = [ redis.set(key, value) for redis in self.redises ]
        await asyncio.gather(*tasks)

    async def _transact(self, redis, code, jmsg, jdata, data):
        async with redis.pipeline(transaction=self.use_transactions) as pipe:
            if code == "match_changed":
                return await (pipe.publish(self.channel, jmsg).hset(self.hash_path, data["id"], jdata).execute())
            elif code == "match_removed":
                return await (pipe.publish(self.channel, jmsg).hdel(self.hash_path, data).execute())
            elif code in ("parser_started", "parser_finished"):
                return await (pipe.publish(self.channel, jmsg).delete(self.hash_path).execute())

# важно чтобы data был последним в объекте. тогда его можно легко резать на приемке!
# поменяли - match_removed->data:str ->  match_removed->mid:int, data теперь бывает только в match_changed, links_changed
#  jmsg, jdata |  bytes, bytes |  json кодированный ивент и json кодированное тело матча.
# msg_ts - это время создания этого сообщения в источнике. чтобы не тратить много места,
# мы прилагаем инфу как количество миллисекунд задержки от создания в источнике до time.time()
def build_message(code, data, msg_ts=None):
    t = time.time()
    msg = {"type": code, "ts": round(t, 3) }
    if msg_ts is not None:
        msg['d'] = int(1000*(t - msg_ts))

    if code in ("parser_started", "parser_finished"):
        return orjson.dumps(msg), None

    # матч поменялся. норм. data - это Match объект. dict короче
    elif code == "match_changed":
        msg['mid'] = int(data['id'])
        msg['data'] = None
        jmsg = orjson.dumps(msg)

        jdata = orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS)            # это кодированный матч
        jmsg = jmsg.replace(b"null", jdata)   # это сообщение с данными

        return jmsg, jdata

    # матч закончился. норм. data - это mid.
    elif code == "match_removed":
        msg['mid'] = int(data)             # почему str ?это же int. наверное для обратной совместимости ?
        return orjson.dumps(msg), None

    raise Exception(f"unsupported {code=}, {data=}")
