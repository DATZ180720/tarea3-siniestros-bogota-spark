"""Publica eventos viales sintéticos en Kafka para la demostración streaming.

Estos eventos son simulados; no provienen de reportes oficiales en vivo.
Requiere: pip install kafka-python
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BROKERS = "localhost:9092"
TOPIC = "siniestros_bogota"
LOCALITIES = ["Kennedy", "Suba", "Engativa", "Chapinero", "Fontibon", "Usaquen"]
CLASSES = ["CHOQUE", "ATROPELLO", "CAIDA_OCUPANTE", "VOLCAMIENTO"]
SEVERITIES = ["SOLO_DANOS", "HERIDOS", "FALLECIDOS"]


def make_event():
    return {
        "event_id": f"sim-{time.time_ns()}",
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "locality": random.choice(LOCALITIES),
        "accident_class": random.choice(CLASSES),
        "severity": random.choices(SEVERITIES, weights=[65, 32, 3])[0],
        "latitude": round(random.uniform(4.50, 4.80), 6),
        "longitude": round(random.uniform(-74.20, -74.00), 6),
        "synthetic": True,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    try:
        while True:
            event = make_event()
            producer.send(TOPIC, key=event["locality"], value=event)
            producer.flush()
            print(f"Evento sintético enviado: {event}", flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Productor detenido.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
