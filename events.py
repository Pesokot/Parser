import aioredis
import asyncio

import time
import ujson

import logging
logger = logging.getLogger('events')

from contextlib import asynccontextmanager

# is_live никак не используется
class EventPublisherPool():
    @classmethod
    @asynccontextmanager
    async def create(cls, redis_list, bookmaker_id=None, *, is_live=True, timeout=5, publisher=None):
        self = EventPublisherPool()
        self.eps = [ await EventPublisher.create(redis, bookmaker_id, is_live=is_live, timeout=timeout, publisher=publisher) for redis in redis_list ]
        try:
            yield self
        finally:
            for ep in self.eps:
                await ep.send("parser_finished")


    async def send(self, message, data=None):
        coroutines = [ ep.send(message, data) for ep in self.eps ]
        await asyncio.gather(*coroutines)

class EventPublisher():
    # асинхронный конструктор
    @classmethod
    async def create(cls, redis, bookmaker_id=None, *, is_live=True, timeout=5, publisher=None):
        self = EventPublisher()

        self.host, self.port, self.password = redis
        self.loop = asyncio.get_event_loop()

        self.timeout = timeout
        self.bookmaker_id = bookmaker_id

        self.live = is_live
        self.publisher = publisher or f"event_publisher for {self.bookmaker_id}"

        self.redis = None
        if bookmaker_id:
            self.event_channel = f"events:{self.bookmaker_id}"
            self.hash_path = f"match:{self.bookmaker_id}"
            await self.send("parser_started")
        return self

    async def send(self, message, data=None):
        if not self.redis or self.redis.closed:
            logger.warning('redis dead, fuck!')
            self.redis = await aioredis.create_redis_pool(f"redis://{self.host}:{self.port}/0?encoding=utf-8", password=self.password, timeout=self.timeout)

        msg = {"type": message, "publisher": self.publisher, "timestamp": time.time(), "bookmaker_id": self.bookmaker_id, "live": self.live, "data": None}
        # это типа пустой тик такой, нужно слать сообщение о старте парсинга, чтобы все перепрочитали стейты
        if message in ("parser_started", "parser_finished"):
            jmsg = await self.loop.run_in_executor(None, ujson.dumps, msg)

            tr = self.redis.multi_exec()
            fut1 = tr.publish(self.event_channel, jmsg)
            fut2 = tr.delete(self.hash_path)
            res = await tr.execute()

        # матч поменялся. норм. data - это Match объект. dict короче
        if message == "match_changed":
            msg["data"] = data['id']
            jmsg = await self.loop.run_in_executor(None, ujson.dumps, msg)
            jdata = await self.loop.run_in_executor(None, ujson.dumps, data)

            tr = self.redis.multi_exec()
            fut1 = tr.publish(self.event_channel, jmsg)
            fut2 = tr.hset(self.hash_path, data["id"], jdata)
            res = await tr.execute()

        # матч закончился. норм. data - это mid.
        if message == "match_removed":
            msg["data"] = data
            jmsg = await self.loop.run_in_executor(None, ujson.dumps, msg)

            tr = self.redis.multi_exec()
            fut1 = tr.publish(self.event_channel, jmsg)
            fut2 = tr.hdel(self.hash_path, data)
            res = await tr.execute()

        # новые данные линковки. data - это LINKS собственно
        if message == "links_changed":
            msg["data"] = data
            jmsg = await self.loop.run_in_executor(None, ujson.dumps, msg)
            jdata = await self.loop.run_in_executor(None, ujson.dumps, data)

            tr = self.redis.multi_exec()
            fut1 = tr.publish("events:links", jmsg)
            fut2 = tr.set("links", jdata)
            res = await tr.execute()

        logger.debug(f"event sent: {msg}")


async def main():
    # сделай в терминале redis-cli psubscribe "events:*"
    R1 = "127.1", 6379, None
    R2 = "127.1", 56379, "password"

    REDIS_LIST = [ R1, R2 ]
    BOOKMAKER_ID = 42

    # можно так, но зачем?
    # ep = await EventPublisher.create(R1, BOOKMAKER_ID)

    # лучше так.
    # EventPublisherPool ведет себя как EventPublisher и скрывает сложность в себе. просто делайте сразу пул из списка реквизитов редисов и не парьтесь.
    epp = await EventPublisherPool.create(REDIS_LIST, BOOKMAKER_ID)
    # в момент создания мы уже пошлем правильную parser_started транзакцию

    await asyncio.sleep(5)

    # осталось только следить за мутациями и звать метод если что
    # допустим получился матч m и он только что мутировал. давайте опубликуем его. не дожидаясь конца отправки.
    m = {"id":4545, "bookmaker_id": BOOKMAKER_ID, "periods_v2": {}}
    asyncio.create_task( epp.send("match_changed", m) )
    await asyncio.sleep(5)

    # ой. матч закончился.
    asyncio.create_task( epp.send("match_removed", 4545) )

    # вот и все. вы великолепны.
    # если мы опять захотим поменять тз и механизм публикаций или придумаем новые события, то просто не парьтесь. пусть парятся эти два объекта.

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
