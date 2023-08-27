import asyncio

from fastapi import FastAPI
from redis.asyncio import Redis

from constants import DOMAIN_HEAP_QUEUE, ACQUIRED_DOMAINS_SET
from models import ReleaseRequestBody
from settings import Settings

app = FastAPI()
settings = Settings()

redis = Redis(host=settings.redis_host, port=settings.redis_port)


@app.get("/acquire")
async def acquire() -> str | None:
    for domain in await redis.zrange(DOMAIN_HEAP_QUEUE, 0, 0):
        if domain is None:
            return None
        elif await redis.sismember(ACQUIRED_DOMAINS_SET, domain):
            continue
        await redis.sadd(ACQUIRED_DOMAINS_SET, domain)
        return domain
    return None


@app.post("/release")
async def release(body: ReleaseRequestBody):
    pipe = redis.pipeline()
    task0 = pipe.zadd(DOMAIN_HEAP_QUEUE, {body.domain: body.timestamp})
    task1 = pipe.srem(ACQUIRED_DOMAINS_SET, body.domain)
    await asyncio.gather(*(task0, task1))
    await pipe.execute()
