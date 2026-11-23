"""Helpers for the end-to-end tests: Kafka, Schema Registry, HDFS (through docker compose) and test logging.

The tests talk to the running dev stack: Kafka on localhost:9092, the Schema Registry on localhost:8085
and HDFS through `docker compose exec namenode hdfs dfs ...` (the DataNodes are not reachable from the host).
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from fastavro import parse_schema, schemaless_reader

REPO = Path(__file__).resolve().parents[2]
LOG_DIR = REPO / "deliverables" / "test_logs"
BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8085"
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

pd.set_option("display.width", 170)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 34)


# --- shell --------------------------------------------------------------------------------------


def run(cmd: list[str], *, input: bytes | None = None, cwd: Path | None = None, check: bool = True,
        timeout: int = 300) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, input=input, capture_output=True, cwd=cwd or REPO, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed ({proc.returncode}): {proc.stderr.decode(errors='replace')[:500]}")
    return proc


def compose(*args: str, input: bytes | None = None, check: bool = True, timeout: int = 300):
    return run(["docker", "compose", *args], input=input, check=check, timeout=timeout)


def compose_text(*args: str, **kw) -> str:
    return compose(*args, **kw).stdout.decode(errors="replace")


# --- time ---------------------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def log_ts(ts: datetime, offset: str = "+0000") -> str:
    """Apache common log timestamp, e.g. 23/Nov/2026:15:20:00 +0000 (month names do not depend on the locale)."""
    return f"{ts.day:02d}/{MONTHS[ts.month - 1]}/{ts.year}:{ts:%H:%M:%S} {offset}"


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


# --- Kafka and the Schema Registry --------------------------------------------------------------


@dataclass
class Msg:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp: int


def produce(topic: str, records: list[tuple[str | None, str]]) -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    for key, value in records:
        p.produce(topic, value=value.encode(), key=key.encode() if key is not None else None)
    left = p.flush(30)
    if left:
        raise RuntimeError(f"{left} messages were not delivered to {topic}")


def end_offsets(topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"e2e-{uuid.uuid4()}"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def consume_all(topic: str, timeout: int = 120) -> list[Msg]:
    """Reads a topic from the beginning up to the end offsets at the time of the call."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"e2e-{uuid.uuid4()}", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        wanted, assign = {}, []
        for p in parts:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                wanted[p] = high
                assign.append(TopicPartition(topic, p, low))
        out: list[Msg] = []
        if not wanted:
            return out
        c.assign(assign)
        done: set[int] = set()
        deadline = time.time() + timeout
        while len(done) < len(wanted) and time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            out.append(Msg(m.partition(), m.offset(), m.key(), m.value(), m.timestamp()[1]))
            if m.offset() + 1 >= wanted[m.partition()]:
                done.add(m.partition())
        return out
    finally:
        c.close()


_schemas: dict[int, object] = {}


def registry_get(path: str):
    r = requests.get(f"{REGISTRY}{path}", timeout=10)
    r.raise_for_status()
    return r.json()


def avro_decode(value: bytes) -> dict:
    """Confluent wire format: magic byte 0, 4-byte schema id, Avro body."""
    if value[0] != 0:
        raise ValueError("not in the Confluent wire format")
    schema_id = int.from_bytes(value[1:5], "big")
    if schema_id not in _schemas:
        _schemas[schema_id] = parse_schema(json.loads(registry_get(f"/schemas/ids/{schema_id}")["schema"]))
    return schemaless_reader(io.BytesIO(value[5:]), _schemas[schema_id])


def avro_records(topic: str) -> list[dict]:
    rows = []
    for m in consume_all(topic):
        rec = avro_decode(m.value)
        rec["_partition"], rec["_key"] = m.partition, m.key.decode() if m.key else None
        rows.append(rec)
    return rows


# --- HDFS ---------------------------------------------------------------------------------------

_parquet_cache: dict[str, pd.DataFrame] = {}


def hdfs_files(path: str) -> list[str]:
    """Finalized files under a directory. Flink writes in-progress files with a leading dot."""
    p = compose("exec", "-T", "namenode", "hdfs", "dfs", "-ls", "-R", path, check=False)
    files = []
    for line in p.stdout.decode(errors="replace").splitlines():
        if line.startswith("-"):
            f = line.split()[-1]
            if not f.rsplit("/", 1)[-1].startswith("."):
                files.append(f)
    return files


def hdfs_cat(path: str) -> bytes:
    return compose("exec", "-T", "namenode", "hdfs", "dfs", "-cat", path).stdout


def read_parquet(path: str) -> pd.DataFrame:
    """All finalized Parquet files under a directory as one DataFrame; dt and hr come from the directory names."""
    frames = []
    for f in hdfs_files(path):
        if f not in _parquet_cache:
            df = pd.read_parquet(io.BytesIO(hdfs_cat(f)))
            m = re.search(r"dt=([^/]+)/hr=([^/]+)/", f)
            df["dt"], df["hr"] = (m.group(1), m.group(2)) if m else (None, None)
            df["_file"] = f.rsplit("/", 1)[-1]
            _parquet_cache[f] = df
        frames.append(_parquet_cache[f])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def read_text_lines(path: str) -> list[str]:
    lines: list[str] = []
    for f in hdfs_files(path):
        lines.extend(hdfs_cat(f).decode(errors="replace").splitlines())
    return lines


def wait_for(predicate, timeout: int, interval: int = 10, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last is not None and last is not False and not (isinstance(last, pd.DataFrame) and last.empty) \
                and (isinstance(last, pd.DataFrame) or bool(last)):
            return last
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}")


# --- test recording -----------------------------------------------------------------------------


@dataclass
class Case:
    id: str
    group: str
    title: str
    objective: str
    steps: list[str]
    expected: str
    status: str = "NOT RUN"
    actual: str = ""
    seconds: float = 0.0
    output: list[str] = field(default_factory=list)
    checks: list[tuple[bool, str]] = field(default_factory=list)

    def out(self, text: str = "") -> None:
        self.output.append(text)

    def df(self, frame: pd.DataFrame, cols: list[str] | None = None, n: int = 5) -> None:
        shown = frame[cols] if cols else frame
        self.out(f"DataFrame shape = {frame.shape}")
        self.out(shown.head(n).to_string(max_colwidth=36))

    def check(self, ok: bool, message: str) -> bool:
        self.checks.append((bool(ok), message))
        self.out(f"[{'ok' if ok else 'FAILED'}] {message}")
        return bool(ok)

    def write(self) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        head = ["", f"Test case : {self.id} - {self.title}", f"Objective : {self.objective}", "Steps:"]
        head += [f"  {i}. {s}" for i, s in enumerate(self.steps, 1)]
        head += [f"Expected  : {self.expected}", "", "----- observed output -----"]
        tail = ["", f"----- result: {self.status} ({self.seconds:.0f} s) -----", f"Actual    : {self.actual}"]
        (LOG_DIR / f"{self.id}.log").write_text("\n".join(head + self.output + tail) + "\n")

    def as_json(self) -> dict:
        return {k: getattr(self, k) for k in ("id", "group", "title", "objective", "steps", "expected", "status",
                                               "actual", "seconds")}


RESULTS: list[Case] = []


def execute(case: Case, fn) -> Case:
    """Runs one test function; an exception is a FAIL with the traceback in the log."""
    started = time.time()
    print(f"==> {case.id}: {case.title}", flush=True)
    try:
        fn(case)
        if not case.checks:
            raise AssertionError("the test performed no checks")
        failed = [m for ok, m in case.checks if not ok]
        case.status = "FAIL" if failed else "PASS"
        if not case.actual:
            case.actual = f"{sum(ok for ok, _ in case.checks)}/{len(case.checks)} checks passed"
        if failed:
            case.actual += " Failed: " + "; ".join(failed)
    except Exception as exc:  # noqa: BLE001 - any error is a test failure, reported in the log
        case.status = "FAIL"
        case.out(traceback.format_exc())
        case.actual = f"Error: {type(exc).__name__}: {exc}"
    case.seconds = time.time() - started
    case.write()
    RESULTS.append(case)
    print(f"    {case.status} ({case.seconds:.0f} s) {case.actual[:150]}", file=sys.stderr, flush=True)
    return case


def save_results() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / "results.json"
    merged = {c["id"]: c for c in json.loads(path.read_text())} if path.exists() else {}
    for c in RESULTS:  # a partial re-run replaces only the cases it ran
        merged[c.id] = c.as_json()
    order = {cid: i for i, cid in enumerate(["FA-01", "FA-02", "FA-03", "FA-04", "FA-05", "FA-06", "FA-07", "FA-08",
                                              "FP-01", "FP-02", "FP-03", "FP-04", "FP-05", "FP-06",
                                              "Q-01", "Q-02", "Q-03", "Q-04", "Q-05", "Q-06", "Q-07"])}
    path.write_text(json.dumps(sorted(merged.values(), key=lambda c: order.get(c["id"], 99)), indent=2, ensure_ascii=False))
