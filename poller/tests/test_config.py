import pytest

from gh_poller.config import load_config


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no .env here: the test must not read a real token
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    for name in ("TIER2_INTERVAL_S", "TIER2_BUDGET_PER_H", "TIER1_INTERVAL_S", "WATCHLIST_PATH", "DATA_DIR"):
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
