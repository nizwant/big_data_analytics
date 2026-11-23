# 2026-10-22 — GitHub poller: v0 review, first token run, v1, pre-commit review

Conversation with Claude, except that local paths are replaced with `<repo>` and `<data-dir>`; replies are condensed. Context-compaction commands are left out.

---

**User:** Implementacja pollera GitHub Events API (stream B) w projekcie Big Data Analytics. 

Kontekst
- Przeczytaj z Projektu: ustalenia + stan v0
- Priorytet oceny: architektura i niezawodność przepływu danych, nie jakość modeli.
- Repo zespołu: nizwant/big_data_analytics, branch feature/github-poller, mój kod tylko w poller/.
- Lokalnie: <repo>.

Stan: v0  
- poller/gh_poller/:
  - config: zmienne środowiskowe / .env
  - github: jeden poll; ETag/If-None-Match, paginacja po nagłówku Link do limitu stron, obsługa 304/404/403/429/5xx
  - state: SQLite; ETagi i dedup po event.id z retencją 2 dni (globalny feed) / 31 dni (watch-lista)
  - sink: dzienny JSONL; format rekordu {fetched_at, track, endpoint, event} definiuje tylko sink.make_record
  - runner: task asyncio na każdy timeline, wspólna pauza przy limitach, jedna linia logu JSON na zapytanie
- Tory: globalny /events (3 strony, co ≥60 s) + 12 orgów R z endpoints.txt (w tym /orgs/cran/events, co 120 s).


Zasady pracy

- Nie zmieniaj plików poza poller/ bez pytania. Nie commituj i nie pushuj
- Nowe decyzje dopisuj do notes/decisions.md i do poller-decisions.md w Projekcie.

 kroki
1. Przejrzyj kod v0 i uruchom testy.
2. Pierwszy przebieg z moim tokenem: w logu odpowiedzi 304, brak 403/429, brak duplikatów

3. zdaj zwięzły raport dot. testów

**Assistant:** Reviewed `poller/` and ran the tests: 18/18 pass on v0. The review found three bugs, each first reproduced with a failing test on the unchanged code:

- **First poll cut an org's history short.** Pagination stopped at the first known event, so if the global feed had already stored an org's newest event, the org's first poll ended after page 1 (99 of 299 events stored). Fix: stop on a known event only when the timeline has a stored ETag.
- **Missing pages were never fetched again.** After an error on page 2, page 1 was already deduplicated, so the next poll stopped there (100 of 300). Fix: an error on pages 2–3 clears the ETag, so the next poll fetches all pages.
- **A dedup-cleanup error stopped the whole poller** (e.g. a locked SQLite file, propagated through `asyncio.gather`). Fix: log and retry in an hour.

Added `scripts/check_run.py`, which reports a run from its output files only (statuses, 403/429, share of 304, whether 304 uses quota, duplicate ids, events/min, delays) and exits 1 on 403/429 or duplicates. 21/21 tests pass. Gave the commands and pass criteria for the token run, to be done locally.

---

**User:** Opcja 2: przebiegi poszly lokalnie na Windowsie. Nie odpalaj pollera u siebie, ignoruj .env
Oceń;

- .env: GITHUB_TOKEN + DATA_DIR=<data-dir> , dodany do gitignore

- Testy: pytest z PATH to był Python 3.10 bez respx, ustalone poller/.venv na 3.13. Tam 21 passed.

dwa przebiegi
Wyniki: 0× 403/429, 0 błędów, 0 duplikatów na 13 057 eventach. Na watch-liście 82% odpowiedzi 304, 304 nie zjada limitu, limit na starcie 4984/5000. Globalny feed ok. 240 eventów/min.
Restart: 9 z 12 orgów od razu dostało 304. cran, rstudio i posit-dev pobrały po 1 stronie z samymi nowymi eventami z przerwy (3, 11, 13). ETagi i dedup przeżyły restart.

Do sprawdzenia:
1. Globalny feed: opóźnienie p50 ok. 5 min, ale p90 ok. 23 h, a max prawie 4 lata (stare created_at). Co to za eventy?
2. Globalny feed to próbka: w każdym pollu 210–260 nowych na 300 pobranych.
3. check_run liczy eventy/min razem z przerwą między przebiegami (raport pokazuje 162/min zamiast ok. 240) i nie czyta DATA_DIR z .env.

Potem dopisz do notes/decisions.md („Stan v0”): wynik testu z 22.10, przebieg lokalny + DATA_DIR poza OneDrive, testy w poller/.venv, liczba testów 21 zamiast 18. Ten sam tekst przygotuj do poller-decisions.md w Projekcie.

**Assistant:** Confirmed the numbers on the run data (read-only; `.env` not opened). The run passes the first test; the beta criterion still needs a several-hour run.

1. **Global-feed delays** (9,556 events): 66.8% arrive after 5–10 min, 13.0% within 10 min – 6 h, 19.5% after 20–48 h. The 20–48 h group is a GitHub backlog: 1,849 Push/Create/Delete events from 20.10 09:40 – 21.10 01:52 UTC, in two batches delayed by ~22.7 h and ~37 h. The ~4-year maximum is not a delay: in `PublicEvent`, `created_at` is the repository's creation date (it grows with `repo.id` in all 32 cases). **For Flink:** event time from `created_at` (`fetched_at` for `PublicEvent`), 10 min watermark, 1 h allowed lateness (keeps 72% of the global feed and 86% of the R track), later events to a side output; exact counts from the daily batch recompute. To be confirmed by a 24 h measurement.
2. **Sample:** "300 fetched, 210–260 new" does not prove overlap between polls: on the first poll 90 of 299 events were duplicates across pages of the same poll, because the feed moves between page requests. Added `dup_in_poll` and `known` to the request log; `known = 0` on a global poll marks a possible gap. 235/min is a lower bound.
3. **`check_run.py`:** rate now counted only over the time the poller ran and only for steady-state polls (235.2/min instead of 162.2); `DATA_DIR` read from argument, environment, or `.env`.

Also added `.env` and `.venv/` to `poller/.gitignore` (previously covered only by the root `.gitignore`). 25 tests. `decisions.md` and the Project doc updated.

---

**User:** Zaimplementuj v1 pollera GitHub (poller/, branch feature/github-poller). Kontekst: notes/decisions.md i notes/github-poller-plan.md . Najpierw krótki plan, potem kroki po kolei + testy brakujace


 retencja dedup w poll_and_store: zapisują się wszystkie id z odpowiedzi, watch-lista trzyma je 31 dni, a retencja nie skraca się, kiedy to samo id przyjdzie później z toru globalnego

Pause i endpoint_loop: po limicie pauza jest wspólna, po błędzie backoff 5 min, X-Poll-Interval jest respektowany, martwy timeline przestaje być odpytywany, a wyjątek w jednym pollu nie wywraca całego pollera;
- liczba ponowień przy błędzie sieci;
- check_run.py: exit 1 przy 403/429, werdykt „304 not charged” i pomijanie PublicEvent w opóźnieniach.

Potem watch-lista. Źródłem jest crantogit.csv z r-universe-org/cran-to-git. Interesują nas tylko biblioteki gdzie repo na GitHubie i puste registry. Z URL-a ekstrakcja owner/repo, pogrupuj po właścicielu.

Jeśli właściciel ma jedno repo, odpytujemy /repos/{o}/{r}/events. Jeśli ma ich więcej, weryfikacja: organizacja vs zwykły użytkownik. Sprawdzić przez GET /users/{name}, wynik zapisany do pliku.

Organizacja: jedno zapytanie /orgs/{o}/events. U użytkownika przejście przez /repos/{o}/{r}/events. Nie używaj /users/{u}/events/public

Wynik zapisz do watchlist.csv z kolumnami package, owner, repo, endpoint, tier. Tier 1 (co 2 min) to duże orgi R i /orgs/cran/events, tier 2 to cała reszta, z interwałem z .env

ustawienia scheduler: kolejka po next_poll_at, token bucket 300 zapytań/min, budżet z X-RateLimit-Remaining, pierwszy przebieg rozłożony w czasie, tier 1 jako pierwszy. Tor globalny /events zostaje osobnym zadaniem. watchlist.csv przeładowanie, gdy zmieni się mtime pliku, bez restartu.

3. Pole packages w rekordzie, po repo.name



**Assistant:** Sent a short plan, then implemented the steps in order, each with tests (84 mocked tests, Python 3.10; nothing run against GitHub):

0. +19 safety tests on the unchanged v0 code.
1. `gh_poller/watchlist.py` (`python -m gh_poller.watchlist`): account types cached in `account_types.csv`, one row per request, so an interrupted build resumes; stops at a rate limit or below 1,000 remaining; atomic write. Today's `crantogit.csv`: 16,923 packages, 16,829 repos, 8,481 owners.
2. `limits.py` (300/min token bucket, budget thresholds, shared pause) and `scheduler.py` (a `heapq` queue per tier, tier 1 first, `Semaphore(10)`, deadlines restored from the last poll after a restart, `watchlist.csv` reloaded on change). The global track uses the same limiter and budget.
3. `packages` in the record for both tracks (`cran/X` → package X).
4. `401` stops the poller and the watch-list build with a clear message.

Proposed for decision: an organisation gets its org timeline only if R repos are ≥ 50% of its public repos (`apache`, `microsoft`, `google`, `azure` would otherwise flood the 300-event window), and a 3,600 s default tier-2 interval.

---

**User:** 1. ORG_MIN_R_SHARE=0.5: OK. Liczbę publicznych repo bierz z public_repos z tej samej odpowiedzi GET /users/{name}, którą sprawdzasz typ konta. Bez dodatkowych zapytań, wszystko w cache.

2. TIER2_INTERVAL_S=3600: nie. Wiążący jest limit 5 000/h, nie 900/min. W pierwszym cyklu każde zapytanie to płatne 200, więc 16 200 endpointów w godzinę daje ponad 3× limit. Do v1.1 (progi X-RateLimit-Remaining) zostaje wzór z promptu: domyślny interwał tieru 2 = liczba endpointów tieru 2 / 3 500 godzin, zaokrąglone w górę (dziś 3–5 h). Skrócenie do 1 h dopiero po pomiarze 24 h, kiedy będzie wiadomo, ile odpowiedzi to 304.

3 oraz 4 okay

**Assistant:** Agreed: the 3,600 s default came only from the 300/min rate and ignored the 5,000/h quota. Tier-2 interval now defaults to ceil(tier-2 timelines / `TIER2_BUDGET_PER_H`) hours with `TIER2_BUDGET_PER_H=3500` (3–5 h today), recomputed on every watch-list reload; an explicit `TIER2_INTERVAL_S` (≥ 900) overrides it, with a log warning if it exceeds the budget. A virtual-time test shows no hour exceeds the budget even when every response is a paid 200. `public_repos` already came from the same cached `GET /users/{name}`; a test now confirms a second build sends no requests. 93 tests.

---

**User:** Before I commit, analyse the results obtained so far, highlight any areas where there may occur shortcomings, vulnerabilties or need for complexity reduction.

**Assistant:** Reviewed the local commit `e626c1d` (not pushed). It is clean: no secrets, `.env` or data files; 90 offline tests pass (live tests excluded, since they read `.env`). Findings:

- **Fix before the 24 h run:**
  1. Tier 2 bursts after any gap (restart, sleep, stop below 200): all overdue timelines go out at 300/min, which can use the whole hourly quota and pause the global track too.
  2. `check_run.py` loads everything into memory: 3.3× the file size, ~4.3 GB for a 24 h run.
  3. A 404/410 on `/events` ends the global track for good while the process keeps running.
  4. (Decision) a 3–5 h tier-2 interval does not fit a 1 h allowed lateness in Flink; tier 2 is effectively batch-layer data.
- **Fix soon:** watch-list timelines can silently lose events beyond 300 per interval; the global feed has no protected share of the quota; live tests run by default and use the token; `watchlist.csv` from Airflow should be validated (path pattern, refuse a reload that drops > 20% of timelines); a crash mid-write can corrupt the last JSONL line; ~1.3 GB/day of disk.
- **Simpler:** five overlapping tier-2 limits could become one page budget plus one stop threshold; `check_run` could read a `first_poll` field instead of inferring it.

**User:** 1) burst: fix it, but limit page requests, not polls
2) keep only the fields relevant report-wise
3) yes

afterwards run pytest

any comments ought to be kept concise

**Assistant:** Implemented all three; 94 offline tests pass.

1. **Tier 2 is capped by paid page requests:** a poll starts only with budget available and is then charged for the pages it paid for (304 = 0, a 3-page poll = 3). Covers the first cycle, restarts, sleep and quota resets the same way; overshoot ≤ burst 10 + ≤ 20 in-flight pages. The first-poll spreading code was removed.
2. **`check_run.py` streams the JSONL** and keeps only report fields: 3.2 MB instead of 165 MB on the 21.10 data, same output.
3. **A 404/410 on `/events` backs off 5 min** instead of ending the track.

README, `.env.example`, `notes/decisions.md` and the Project doc updated; changes left uncommitted on top of `e626c1d` for review.
