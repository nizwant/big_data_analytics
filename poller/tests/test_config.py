import pytest

from gh_poller.config import load_config


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no .env here: the test must not read a real token
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    for name in (
        "TIER2_INTERVAL_S", "TIER2_BUDGET_PER_H", "TIER1_INTERVAL_S", "WATCHLIST_PATH", "DATA_DIR",
        "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC", "EVENTS_JSONL", "WATCHLIST_SOURCE",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_tier2_interval_is_automatic_unless_set(env):
    cfg = load_config()
    assert cfg.tier2_interval_s is None and cfg.tier2_budget_per_h == 3500
    env.setenv("TIER2_INTERVAL_S", "")
    assert load_config().tier2_interval_s is None
    env.setenv("TIER2_INTERVAL_S", "3600")  # after the 24 h measurement
    assert load_config().tier2_interval_s == 3600


@pytest.mark.parametrize("name, value", [("TIER2_INTERVAL_S", "600"), ("TIER2_BUDGET_PER_H", "6000"), ("TIER1_INTERVAL_S", "30")])
def test_invalid_settings_stop_the_start(env, name, value):
    env.setenv(name, value)
    with pytest.raises(SystemExit, match=name):
        load_config()


def test_outputs_default_to_jsonl_and_the_team_topic(env):
    cfg = load_config()
    assert cfg.events_jsonl and cfg.kafka_bootstrap_servers is None
    assert cfg.kafka_topic == "github_repo_updates"  # created by deploy/kafka_init.sh
    env.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    env.setenv("EVENTS_JSONL", "false")
    cfg = load_config()
    assert cfg.kafka_bootstrap_servers == "kafka:19092" and not cfg.events_jsonl


def test_no_output_stops_the_start(env):
    env.setenv("EVENTS_JSONL", "false")
    with pytest.raises(SystemExit, match="no output"):
        load_config()


def test_watch_list_comes_from_kafka_when_kafka_is_set(env):
    assert load_config().watchlist_source == "file"
    env.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    assert load_config().watchlist_source == "kafka"
    env.setenv("WATCHLIST_SOURCE", "file")  # e.g. a hand-made list while Kafka takes the events
    assert load_config().watchlist_source == "file"


@pytest.mark.parametrize("kafka, source, message", [("", "kafka", "needs KAFKA_BOOTSTRAP_SERVERS"), ("kafka:19092", "hdfs", "kafka or file")])
def test_invalid_watch_list_source_stops_the_start(env, kafka, source, message):
    env.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka)
    env.setenv("WATCHLIST_SOURCE", source)
    with pytest.raises(SystemExit, match=message):
        load_config()


def test_settings_can_come_from_a_mapping_instead_of_the_environment(env):
    cfg = load_config(env_file=None, environ={"GITHUB_TOKEN": "from-airflow", "DATA_DIR": "/tmp/build"})
    assert cfg.github_token == "from-airflow" and str(cfg.watchlist_path) == "/tmp/build/watchlist.csv"
