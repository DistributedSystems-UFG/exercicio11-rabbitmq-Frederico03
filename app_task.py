import argparse
import json
import random
import sqlite3
import time
from datetime import datetime, timezone

import pika

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
RABBITMQ_VHOST = "/"

Q_RAW_A = "temperature.raw.a"
Q_RAW_B = "temperature.raw.b"
Q_PROCESSED = "temperature.processed"
Q_ALERTS = "temperature.alerts"

DB_PATH = "temperature_rabbitmq.db"
ALERT_THRESHOLD = 30.0


def get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
    )
    return pika.BlockingConnection(params)


def ensure_queues(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.queue_declare(queue=Q_RAW_A, durable=True)
    channel.queue_declare(queue=Q_RAW_B, durable=True)
    channel.queue_declare(queue=Q_PROCESSED, durable=True)
    channel.queue_declare(queue=Q_ALERTS, durable=True)


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                source_queue TEXT NOT NULL,
                source_timestamp TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                temperature REAL NOT NULL
            )
            """
        )
        conn.commit()


def run_sensor_producer(sensor_id: str, queue_name: str) -> None:
    conn = get_connection()
    channel = conn.channel()
    ensure_queues(channel)

    current_temp = 25.0
    print(f"Produtor {sensor_id} publicando em {queue_name}")

    try:
        while True:
            delta = random.uniform(-1.2, 1.2)
            current_temp += delta

            if abs(delta) >= 0.5:
                event = {
                    "sensor_id": sensor_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "temperature": round(current_temp, 2),
                }
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=json.dumps(event),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print(f"Publicado em {queue_name}: {event}")

            time.sleep(2)
    except KeyboardInterrupt:
        print("Encerrando produtor...")
    finally:
        conn.close()


def run_processor(input_queue: str) -> None:
    conn = get_connection()
    channel = conn.channel()
    ensure_queues(channel)

    print(f"Processor consumindo {input_queue}")

    def callback(ch, method, properties, body):
        event = json.loads(body.decode("utf-8"))
        temp = float(event["temperature"])

        processed = {
            "sensor_id": event["sensor_id"],
            "source_queue": input_queue,
            "source_timestamp": event["timestamp"],
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "temperature": temp,
        }
        ch.basic_publish(
            exchange="",
            routing_key=Q_PROCESSED,
            body=json.dumps(processed),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        if temp >= ALERT_THRESHOLD:
            alert = {
                "sensor_id": event["sensor_id"],
                "temperature": temp,
                "level": "HIGH",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            ch.basic_publish(
                exchange="",
                routing_key=Q_ALERTS,
                body=json.dumps(alert),
                properties=pika.BasicProperties(delivery_mode=2),
            )

        print(f"Processado ({input_queue}) -> {Q_PROCESSED}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=input_queue, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Encerrando processor...")
    finally:
        conn.close()


def run_storage_consumer() -> None:
    init_db()
    conn = get_connection()
    channel = conn.channel()
    ensure_queues(channel)

    print(f"Storage consumindo {Q_PROCESSED}")

    def callback(ch, method, properties, body):
        event = json.loads(body.decode("utf-8"))
        with sqlite3.connect(DB_PATH) as db:
            db.execute(
                """
                INSERT INTO processed_events
                (sensor_id, source_queue, source_timestamp, processed_at, temperature)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event["sensor_id"],
                    event["source_queue"],
                    event["source_timestamp"],
                    event["processed_at"],
                    float(event["temperature"]),
                ),
            )
            db.commit()

        print("Registro salvo no SQLite")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=Q_PROCESSED, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Encerrando storage...")
    finally:
        conn.close()


def run_alert_consumer() -> None:
    conn = get_connection()
    channel = conn.channel()
    ensure_queues(channel)

    print(f"Alert consumer consumindo {Q_ALERTS}")

    def callback(ch, method, properties, body):
        event = json.loads(body.decode("utf-8"))
        print(
            f"ALERTA: sensor={event['sensor_id']} temp={event['temperature']} nivel={event['level']}"
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=Q_ALERTS, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Encerrando alert consumer...")
    finally:
        conn.close()


def show_history(limit: int) -> None:
    init_db()
    with sqlite3.connect(DB_PATH) as db:
        rows = db.execute(
            """
            SELECT id, sensor_id, source_queue, source_timestamp, processed_at, temperature
            FROM processed_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    if not rows:
        print("Nenhum registro salvo ainda.")
        return

    for row in rows:
        print(
            f"id={row[0]} sensor={row[1]} queue={row[2]} src_ts={row[3]} "
            f"processed_at={row[4]} temp={row[5]}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="RabbitMQ app minimalista")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("producer-a")
    sub.add_parser("producer-b")
    sub.add_parser("processor-a")
    sub.add_parser("processor-b")
    sub.add_parser("storage")
    sub.add_parser("alerts")

    history = sub.add_parser("history")
    history.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    if args.command == "producer-a":
        run_sensor_producer("sensor-a", Q_RAW_A)
    elif args.command == "producer-b":
        run_sensor_producer("sensor-b", Q_RAW_B)
    elif args.command == "processor-a":
        run_processor(Q_RAW_A)
    elif args.command == "processor-b":
        run_processor(Q_RAW_B)
    elif args.command == "storage":
        run_storage_consumer()
    elif args.command == "alerts":
        run_alert_consumer()
    elif args.command == "history":
        limit = args.limit if args.limit > 0 else 10
        show_history(limit)


if __name__ == "__main__":
    main()
