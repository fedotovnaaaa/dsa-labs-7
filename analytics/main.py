import asyncio
import json
import os
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException


def new_store(store_id):
    return {
        "store_id": store_id,
        "purchases": 0,
        "returns": 0,
        "redeems": 0,
        "rejected": 0,
        "points_accrued": 0,
        "points_returned": 0,
        "points_redeemed": 0,
    }


async def consume(app, queue):
    try:
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process(requeue=True):
                    event = json.loads(message.body.decode("utf-8"))
                    op_id = event["operation_id"]
                    if op_id in app.state.seen:
                        continue
                    app.state.received += 1
                    stats = app.state.stores.setdefault(
                        event["store_id"], new_store(event["store_id"])
                    )
                    if event["status"] == "rejected":
                        stats["rejected"] += 1
                    elif event["type"] == "purchase":
                        stats["purchases"] += 1
                        stats["points_accrued"] += event["points_delta"]
                    elif event["type"] == "return":
                        stats["returns"] += 1
                        stats["points_returned"] += abs(event["points_delta"])
                    elif event["type"] == "redeem":
                        stats["redeems"] += 1
                        stats["points_redeemed"] += abs(event["points_delta"])
                    app.state.seen.add(op_id)
                    print(f'Аналитика {event["store_id"]}: {event["type"]} '
                          f'{event["status"]}', flush=True)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print("Ошибка получения событий: " + str(exc), flush=True)
        raise

@asynccontextmanager
async def lifespan(app):
    app.state.stores = {}
    app.state.received = 0
    app.state.seen = set()
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "loyalty.events", aio_pika.ExchangeType.FANOUT, durable=True
    )
    queue = await channel.declare_queue(
        "analytics.events", durable=True, arguments={"x-queue-type": "quorum"}
    )
    await queue.bind(exchange)
    task = asyncio.create_task(consume(app, queue))
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await connection.close()


app = FastAPI(title="Аналитика", lifespan=lifespan)


@app.get("/stores")
async def stores():
    return list(app.state.stores.values())


@app.get("/stores/{store_id}")
async def store(store_id: str):
    if store_id not in app.state.stores:
        raise HTTPException(status_code=404, detail="Магазин не найден")
    return app.state.stores[store_id]


@app.get("/stats")
async def stats():
    return {"received_events": app.state.received}
