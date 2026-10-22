import os

from gh_poller.limits import Pause
from gh_poller.runner import Context, reload_watchlist
from gh_poller.scheduler import Scheduler
from gh_poller.sink import DailyJsonl
from gh_poller.state import State

from helpers import make_cfg

HEADER = "package,owner,repo,endpoint,tier\n"


async def never_called(*_):
    raise AssertionError("the scheduler is not running in these tests")


def write(path, rows, mtime):
    path.write_text(HEADER + "".join(rows), encoding="utf-8")
    os.utime(path, (mtime, mtime))


def test_watchlist_file_is_reloaded_when_it_changes(tmp_path, caplog):
    cfg = make_cfg(tmp_path)
    state = State(cfg.db_path)
    ctx = Context(cfg, None, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause())
    sched = Scheduler(never_called, tier1_interval_s=120, tier2_interval_s=None, tier2_budget_per_h=3500)
    path = cfg.watchlist_path

    assert reload_watchlist(ctx, sched, None) is None and sched.endpoints == {}  # no file yet

    write(path, ["dplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\n", "solo,alice,solo,/repos/alice/solo/events,2\n"], 1000)
    sig = reload_watchlist(ctx, sched, None)
    assert sched.endpoints == {"/orgs/tidyverse/events": 1, "/repos/alice/solo/events": 2}
    assert ctx.watchlist.packages_for("tidyverse/dplyr") == ["dplyr"]
    assert reload_watchlist(ctx, sched, sig) == sig  # unchanged

    write(path, ["dplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\n", "new,bob,new,/repos/bob/new/events,2\n"], 2000)
    sig = reload_watchlist(ctx, sched, sig)
    assert sched.endpoints == {"/orgs/tidyverse/events": 1, "/repos/bob/new/events": 2}
    assert ctx.watchlist.packages_for("bob/new") == ["new"]

    write(path, ["half a line"], 3000)  # broken file: keep polling the current list
    broken = reload_watchlist(ctx, sched, sig)
    assert broken != sig
    assert sched.endpoints == {"/orgs/tidyverse/events": 1, "/repos/bob/new/events": 2}
    assert "keeping the current one" in caplog.text

    path.unlink()  # file gone (e.g. being replaced): keep the current list
    assert reload_watchlist(ctx, sched, broken) == broken
    assert sched.endpoints == {"/orgs/tidyverse/events": 1, "/repos/bob/new/events": 2}
    state.close()
