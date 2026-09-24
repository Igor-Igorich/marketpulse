import asyncio
import os

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError


async def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    admin = None
    for attempt in range(1, 11):
        try:
            admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
            await admin.start()
            break
        except KafkaConnectionError:
            print(f"Kafka ещё не готова (попытка {attempt}/10), жду 3с...")
            await asyncio.sleep(3)
    if admin is None:
        raise RuntimeError("Не удалось подключиться к Kafka за 10 попыток")

    try:
        await admin.create_topics(
            [
                NewTopic(
                    name="raw_trades", num_partitions=3, replication_factor=1
                )
            ]
        )
        print("Топик raw_trades создан: 3 партиции")
    except Exception as e:
        print(f"Топик уже существует или ошибка: {e}")
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
