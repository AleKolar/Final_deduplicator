import aio_pika
import orjson
import asyncio


async def send_test_message():
    try:
        # Подключение с таймаутом
        connection = await aio_pika.connect(
            "amqp://guest:guest@localhost/",
            timeout=5.0
        )

        async with connection:
            channel = await connection.channel()

            # Удаляем очередь если существует
            try:
                await channel.queue_delete("events")
            except aio_pika.exceptions.ChannelClosed:
                pass

            # Создаем очередь с правильными параметрами
            queue = await channel.declare_queue(
                name="events",
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": "dead_letters"
                }
            )

            # Создаем dead letter exchange
            await channel.declare_exchange(
                name="dead_letters",
                type="direct",
                durable=True
            )

            # Отправка сообщения
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=orjson.dumps({
                        "event_hash": "test123",
                        "event_name": "test_event",
                        "event_datetime": "2024-01-01T00:00:00"
                    }),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="events"
            )
            print("✅ Test message sent successfully")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(send_test_message())