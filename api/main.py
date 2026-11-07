import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Literal

import aio_pika
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


class IncomingOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: str = Field(min_length=1)
    type: Literal["purchase", "return", "redeem"]
    customer_id: str = Field(min_length=1)
    store_id: str = Field(min_length=1)
    check_number: str = Field(min_length=1)
    occurred_at: datetime
    amount: Decimal | None = None
    points: int | None = None

    @model_validator(mode="after")
    def validate_fields(self):
        if self.type in ("purchase", "redeem"):
            if self.amount is None or self.amount <= 0:
                raise ValueError("amount должен быть положительным")
        elif self.amount is not None:
            raise ValueError("В возврате amount отсутствует")

        if self.type == "redeem":
            if self.points is None or self.points <= 0:
                raise ValueError("points должен быть положительным целым")
        elif self.points is not None:
            raise ValueError("points допустим только для redeem")
        return self


@asynccontextmanager
async def lifespan(app):
    app.state.engine = create_async_engine(os.environ["DATABASE_URL"])
    app.state.connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    app.state.channel = await app.state.connection.channel(
        publisher_confirms=True, on_return_raises=True
    )
    app.state.exchange = await app.state.channel.declare_exchange(
        "loyalty", aio_pika.ExchangeType.DIRECT, durable=True
    )
    queue = await app.state.channel.declare_queue(
        "operations", durable=True, arguments={"x-queue-type": "quorum"}
    )
    await queue.bind(app.state.exchange, routing_key="operation")
    try:
        yield
    finally:
        await app.state.connection.close()
        await app.state.engine.dispose()


app = FastAPI(title="Бонусная программа", lifespan=lifespan)


@app.post("/operations", status_code=202)
async def post_operation(operation: IncomingOperation):
    payload = operation.model_dump(mode="json")
    try:
        confirmation = await app.state.exchange.publish(
            aio_pika.Message(
                body=json.dumps(payload).encode("utf-8"),
                content_type="application/json",
                message_id=operation.operation_id,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="operation",
            mandatory=True,
        )
        if confirmation is False:
            raise RuntimeError("Нет подтверждения RabbitMQ")
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Брокер недоступен") from exc
    return {"operation_id": operation.operation_id}


@app.get("/operations/{operation_id}")
async def get_operation(operation_id: str):
    async with app.state.engine.connect() as conn:
        row = (await conn.execute(text("""
            SELECT operation_id, status, points_delta, balance_after, reject_reason
            FROM operations WHERE operation_id = :id
        """), {"id": operation_id})).mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Операция ещё не обработана")
    return dict(row)


@app.get("/accounts/{customer_id}")
async def get_account(customer_id: str):
    async with app.state.engine.connect() as conn:
        row = (await conn.execute(text("""
            SELECT customer_id, balance FROM accounts WHERE customer_id = :id
        """), {"id": customer_id})).mappings().first()
        if row is None:
            raise HTTPException(status_code=404, detail="Счёт не найден")
        history = (await conn.execute(text("""
            SELECT operation_id, type, store_id, check_number, amount, points,
                   points_delta, balance_after, status, reject_reason,
                   occurred_at, processed_at
            FROM operations WHERE customer_id = :id
            ORDER BY processed_at DESC, operation_id DESC LIMIT 20
        """), {"id": customer_id})).mappings().all()
    return {
        "customer_id": row["customer_id"],
        "balance": row["balance"],
        "operations": [dict(op) for op in history],
    }


@app.get("/health")
async def health():
    try:
        async with asyncio.timeout(3):
            async with app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            channel = await app.state.connection.channel()
            await channel.close()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="База или брокер недоступны") from exc
    return {"status": "ok"}
