import asyncio
import json
import os
import random
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI


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
                    if event["customer_id"] not in app.state.unsubscribed:
                        if event["status"] == "rejected":
                            message_text = f'Операция отклонена: {event["reject_reason"]}'
                        elif event["points_delta"] >= 0:
                            message_text = (f'Начислено {event["points_delta"]} баллов, '
                                            f'баланс {event["balance_after"]}')
                        else:
                            message_text = (f'Списано {-event["points_delta"]} баллов, '
                                            f'баланс {event["balance_after"]}')
                        await asyncio.sleep(
                            random.uniform(app.state.min_delay, app.state.max_delay) / 1000
                        )
                        notification = {
                            "operation_id": op_id,
                            "customer_id": event["customer_id"],
                            "text": message_text,
                        }
                        app.state.notifications.append(notification)
                        app.state.sent += 1
                        print(f'Уведомление {event["customer_id"]}: {message_text}', flush=True)
                    app.state.seen.add(op_id)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print("Ошибка получения событий: " + str(exc), flush=True)
        raise

@asynccontextmanager
async def lifespan(app):
    app.state.notifications = []
    app.state.received = 0
    app.state.sent = 0
    app.state.seen = set()
    app.state.unsubscribed = set(filter(None, (
        item.strip() for item in os.getenv("UNSUBSCRIBED", "").split(",")
    )))
    app.state.min_delay = int(os.getenv("MIN_DELAY_MS", "200"))
    app.state.max_delay = int(os.getenv("MAX_DELAY_MS", "1500"))

    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "loyalty.events", aio_pika.ExchangeType.FANOUT, durable=True
    )
    queue = await channel.declare_queue(
        "notifier.events", durable=True, arguments={"x-queue-type": "quorum"}
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


app = FastAPI(title="Уведомления", lifespan=lifespan)


@app.get("/notifications")
async def notifications(customer_id: str):
    return [item for item in app.state.notifications
            if item["customer_id"] == customer_id]


@app.get("/stats")
async def stats():
    return {"received_events": app.state.received,
            "sent_notifications": app.state.sent}
