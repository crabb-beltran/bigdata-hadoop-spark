"""
Kafka producer: simulates vehicles passing through ANI toll stations.

Same producer pattern as the course's Anexo 3 (kafka_producer.py), applied to the
batch dataset's domain. Toll names, category mix and evasion hotspots come from
the batch analysis; tariffs and evasion probabilities are simulated values.
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

TOLLS = [
    "ANDES", "CHUSACA", "GUARNE", "RIO BOGOTA", "EL ROBLE",
    "TURBACO", "CHINAUTA", "SIBERIA", "PUENTE AMARILLO", "SAN PEDRO",
]
# Category share of traffic (batch result) and simulated tariff in COP.
CATEGORIES = {
    "I": (0.63, 15_000), "II": (0.13, 19_000), "III": (0.06, 40_000),
    "IV": (0.04, 52_000), "V": (0.05, 64_000), "VI": (0.01, 76_000),
    "VII": (0.02, 85_000), "ESPECIALES": (0.06, 8_000),
}
# Simulated evasion probability; batch hotspots get a higher one.
EVASION_HOTSPOTS = {"PUENTE AMARILLO": 0.08, "SAN PEDRO": 0.08}
BASE_EVASION = 0.01


def generate_event():
    toll = random.choice(TOLLS)
    category = random.choices(
        list(CATEGORIES), weights=[w for w, _ in CATEGORIES.values()]
    )[0]
    return {
        "event_id": str(uuid.uuid4()),
        "peaje": toll,
        "categoria": category,
        "valor_tarifa": CATEGORIES[category][1],
        "evade": random.random() < EVASION_HOTSPOTS.get(toll, BASE_EVASION),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def main():
    parser = argparse.ArgumentParser(description="Simulated toll events producer")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="toll_events")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending to '{args.topic}' at {args.rate} events/s (Ctrl+C to stop)")
    try:
        while True:
            event = generate_event()
            # Key = toll: all events of a toll land in the same partition (ordered).
            producer.send(args.topic, key=event["peaje"], value=event)
            print(f"Sent: {event}")
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()