import os
import random
import re
import signal
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ.get("KAFKA_TOPIC", "cran_mirror_logs")
EVENTS_PER_SECOND = 4
LOG_FILE = "/data/cran_logs_1h.txt"
TIMESTAMP_JITTER_SECONDS = 2
TIMESTAMP_RE = re.compile(r"\[[^\]]+\]")

running = True


def stop(*_):
    global running
    running = False


def with_current_timestamp(line):
    jitter = random.uniform(-TIMESTAMP_JITTER_SECONDS, 0)
    ts = datetime.now(timezone.utc) + timedelta(seconds=jitter)
    return TIMESTAMP_RE.sub(ts.strftime("[%d/%b/%Y:%H:%M:%S +0000]"), line, count=1)


def on_delivery(err, _msg):
    if err is not None:
        print(f"Delivery failed: {err}", flush=True)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    with open(LOG_FILE, encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"Sending random lines from {LOG_FILE} ({len(lines)} lines) to {TOPIC} "
          f"at ~{EVENTS_PER_SECOND} events/s", flush=True)

    sent = 0
    while running:
        line = with_current_timestamp(random.choice(lines))
        producer.produce(TOPIC, line.encode("utf-8"), on_delivery=on_delivery)
        producer.poll(0)
        sent += 1
        if sent % 1000 == 0:
            print(f"Sent {sent} events", flush=True)
        # Exponential inter-arrival times give a Poisson process with mean rate EVENTS_PER_SECOND.
        time.sleep(random.expovariate(EVENTS_PER_SECOND))

    producer.flush(10)


if __name__ == "__main__":
    main()
