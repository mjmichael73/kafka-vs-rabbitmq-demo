"""Send one JSON event through Kafka and RabbitMQ for a local comparison."""

import json
import os
import uuid
from typing import Any

import pika
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_TOPIC = "comparison-events"
RABBITMQ_QUEUE = "comparison-events"


def make_event() -> dict[str, Any]:
    """Create the shared event payload used by both broker adapters."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.created",
        "order_id": "order-1042",
        "amount": 42.50,
        "currency": "USD",
    }


def publish_and_read_kafka(event: dict[str, Any]) -> dict[str, Any]:
    """Write one event and read it with a fresh Kafka consumer group."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        try:
            admin.create_topics(
                [NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all")
    try:
        producer.send(KAFKA_TOPIC, value=json.dumps(event).encode("utf-8")).get(
            timeout=15
        )
    finally:
        producer.close()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=f"comparison-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=20_000,
    )
    try:
        for message in consumer:
            return json.loads(message.value.decode("utf-8"))
        raise TimeoutError("Kafka consumer did not receive the event in 20 seconds")
    finally:
        consumer.close()


def publish_and_read_rabbitmq(event: dict[str, Any]) -> dict[str, Any]:
    """Publish one durable message and acknowledge it after reading."""
    parameters = pika.URLParameters(
        os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    )
    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.queue_purge(queue=RABBITMQ_QUEUE)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(event).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json", delivery_mode=2
            ),
            mandatory=True,
        )
        method, _properties, body = channel.basic_get(
            queue=RABBITMQ_QUEUE, auto_ack=False
        )
        if method is None:
            raise TimeoutError("RabbitMQ queue did not return the published event")
        received = json.loads(body.decode("utf-8"))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return received
    finally:
        connection.close()


def main() -> None:
    """Run the same event through both local brokers and print each result."""
    event = make_event()
    kafka_received = publish_and_read_kafka(event)
    rabbitmq_received = publish_and_read_rabbitmq(event)
    print(f"Kafka received: {json.dumps(kafka_received, sort_keys=True)}")
    print(f"RabbitMQ received: {json.dumps(rabbitmq_received, sort_keys=True)}")


if __name__ == "__main__":
    main()
