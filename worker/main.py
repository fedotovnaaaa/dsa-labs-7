import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import aio_pika
from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from domain import calculate


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    customer_id: Mapped[str] = mapped_column(String, primary_key=True)
    balance: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Operation(Base):
    __tablename__ = "operations"

    operation_id: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    customer_id: Mapped[str] = mapped_column(ForeignKey("accounts.customer_id"), nullable=False)
    store_id: Mapped[str] = mapped_column(String, nullable=False)
    check_number: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    balance_after: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    reject_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def event_for(op):
    return {
        "operation_id": op.operation_id,
        "type": op.type,
        "customer_id": op.customer_id,
        "store_id": op.store_id,
        "status": op.status,
        "points_delta": op.points_delta,
        "balance_after": op.balance_after,
        "reject_reason": op.reject_reason,
        "processed_at": op.processed_at.isoformat(),
    }


async def save_operation(data, sessions):
    async with sessions() as session:
        async with session.begin():
            existing = await session.get(Operation, data["operation_id"])
            if existing is not None:
                return event_for(existing)

            account = await session.get(Account, data["customer_id"])
            if account is None:
                account = Account(customer_id=data["customer_id"], balance=0)
                session.add(account)
                await session.flush()

            purchase = (await session.scalars(
                select(Operation).where(
                    Operation.type == "purchase",
                    Operation.status == "applied",
                    Operation.check_number == data["check_number"],
                )
            )).first()

            already_returned = False
            if data["type"] == "return" and purchase is not None:
                already_returned = (await session.scalars(
                    select(Operation.operation_id).where(
                        Operation.type == "return",
                        Operation.status == "applied",
                        Operation.check_number == data["check_number"],
                    )
                )).first() is not None

            delta, status, reason = calculate(
                data["type"],
                Decimal(data["amount"]) if data.get("amount") is not None else None,
                data.get("points"),
                {"customer_id": account.customer_id, "balance": account.balance},
                {"customer_id": purchase.customer_id, "points_delta": purchase.points_delta}
                if purchase is not None else None,
                already_returned,
            )
            account.balance += delta
            op = Operation(
                operation_id=data["operation_id"],
                type=data["type"],
                customer_id=data["customer_id"],
                store_id=data["store_id"],
                check_number=data["check_number"],
                amount=Decimal(data["amount"]) if data.get("amount") is not None else None,
                points=data.get("points"),
                points_delta=delta,
                balance_after=account.balance,
                status=status,
                reject_reason=reason,
                occurred_at=datetime.fromisoformat(data["occurred_at"].replace("Z", "+00:00")),
                processed_at=datetime.now(timezone.utc),
            )
            session.add(op)
            await session.flush()
            result = event_for(op)
    return result


async def main():
    if os.path.exists("/tmp/worker-ready"):
        os.unlink("/tmp/worker-ready")
    engine = create_async_engine(os.environ["DATABASE_URL"])
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            "loyalty", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await channel.declare_queue(
            "operations", durable=True, arguments={"x-queue-type": "quorum"}
        )
        await queue.bind(exchange, routing_key="operation")

        events = await channel.declare_exchange(
            "loyalty.events", aio_pika.ExchangeType.FANOUT, durable=True
        )
        for name in ("notifier.events", "analytics.events"):
            q = await channel.declare_queue(
                name, durable=True, arguments={"x-queue-type": "quorum"}
            )
            await q.bind(events)

        open("/tmp/worker-ready", "w").close()
        print("Worker запущен: таблицы и очереди готовы", flush=True)

        async with queue.iterator() as messages:
            async for message in messages:
                # При ошибке публикации сообщение возвращается в очередь.
                async with message.process(requeue=True):
                    data = json.loads(message.body.decode("utf-8"))
                    result = await save_operation(data, sessions)  # транзакция уже зафиксирована
                    confirmation = await events.publish(
                        aio_pika.Message(
                            body=json.dumps(result).encode("utf-8"),
                            content_type="application/json",
                            message_id=result["operation_id"],
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key="",
                        mandatory=True,
                    )
                    if confirmation is False:
                        raise RuntimeError("Брокер не подтвердил событие")
                    print(f'{result["operation_id"]}: {result["status"]} '
                          f'({result["points_delta"]:+d})', flush=True)
                    # ACK выполняется при успешном выходе из message.process().
    finally:
        await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
