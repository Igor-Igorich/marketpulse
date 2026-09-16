import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def main() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin.start()
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
