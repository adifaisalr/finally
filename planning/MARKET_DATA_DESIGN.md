# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem: the unified interface, the shared price cache, the GBM simulator, the Massive (Polygon.io) REST client, the SSE stream, and how it plugs into the rest of the backend.

Everything lives under `backend/app/market/`. This document supersedes `planning/archive/MARKET_DATA_DESIGN.md`: it folds in the fixes from `archive/MARKET_DATA_REVIEW.md`, matches the code as built, and adds the pieces the rest of PLAN.md needs but the first design left open (daily-change baseline, ticker validation, positions-aware watchlist removal). Items that are **new relative to the current code** are marked `[NEW]` so an implementer knows what to change.

---

## Table of Contents

1. [Goals & Constraints](#1-goals--constraints)
2. [Architecture](#2-architecture)
3. [File Layout](#3-file-layout)
4. [Data Model — `models.py`](#4-data-model)
5. [Price Cache — `cache.py`](#5-price-cache)
6. [Unified Interface — `interface.py`](#6-unified-interface)
7. [Seed Data — `seed_prices.py`](#7-seed-data)
8. [Simulator — `simulator.py`](#8-simulator)
9. [Massive Client — `massive_client.py`](#9-massive-client)
10. [Factory — `factory.py`](#10-factory)
11. [SSE Stream — `stream.py`](#11-sse-stream)
12. [App Integration](#12-app-integration)
13. [Watchlist & Position Coordination](#13-watchlist--position-coordination)
14. [Error Handling & Edge Cases](#14-error-handling--edge-cases)
15. [Testing](#15-testing)
16. [Configuration Reference](#16-configuration-reference)
17. [Worked Examples](#17-worked-examples)

---

## 1. Goals & Constraints

From PLAN.md §6, §8, §10:

| Requirement | Design response |
|---|---|
| Two data sources, one interface | `MarketDataSource` ABC; factory picks by `MASSIVE_API_KEY` |
| Downstream code is source-agnostic | Sources *push* into `PriceCache`; consumers only read the cache |
| Simulator: GBM, ~500 ms ticks, correlated, random 2–5% events, realistic seeds | `GBMSimulator` with Cholesky-correlated draws and shock events |
| Massive: REST polling, free tier 5 req/min | One snapshot call for *all* tickers, 15 s default interval |
| SSE `GET /api/stream/prices`, ~500 ms cadence | Poll-and-push generator keyed on a cache version counter |
| Watchlist shows price, **daily change %**, sparkline | Cache tracks a per-ticker **day baseline** `[NEW]` (see §4) |
| Trades fill at current price | Trade code calls `cache.get_price()` |
| Portfolio valuation | Same cache, includes tickers held but no longer watched (§13) |
| Dynamic add/remove from UI or LLM | `add_ticker` / `remove_ticker` on the source |

Non-goals: historical bars, order books, extended-hours logic, multi-process sharing (single process, single user).

---

## 2. Architecture

```
                 ┌──────────────────────────────────────────┐
 env MASSIVE_API_KEY ─►  create_market_data_source(cache)    │
                 └───────────────┬──────────────────────────┘
                                 │ returns (unstarted)
                    MarketDataSource  (ABC)
                    ├── SimulatorDataSource ── GBMSimulator (numpy, Cholesky)
                    └── MassiveDataSource  ── massive.RESTClient (sync, in thread)
                                 │  writes: cache.update(ticker, price, ...)
                                 ▼
                          PriceCache  (threading.Lock, version counter)
                                 │  reads
        ┌────────────────────────┼─────────────────────────┐
        ▼                        ▼                         ▼
  SSE /api/stream/prices   Portfolio valuation       Trade execution
  (every 500 ms, on        (GET /api/portfolio,      (POST /api/portfolio/trade,
   version change)          snapshots task)           LLM trades)
```

Key principle: **producers and consumers never talk to each other.** The simulator ticks at 500 ms, Massive polls at 15 s, SSE emits at 500 ms; the cache absorbs the mismatch. Swapping or adding a source touches nothing downstream.

Concurrency model: everything runs on the single asyncio event loop except the blocking Massive HTTP call, which runs via `asyncio.to_thread`. Because the cache is therefore written from a worker thread *and* the loop, it uses `threading.Lock` (not `asyncio.Lock`).

---

## 3. File Layout

```
backend/
  app/
    market/
      __init__.py          # public re-exports
      models.py            # PriceUpdate (frozen dataclass)
      cache.py             # PriceCache
      interface.py         # MarketDataSource ABC
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py         # GBMSimulator + SimulatorDataSource
      massive_client.py    # MassiveDataSource
      factory.py           # create_market_data_source()
      stream.py            # create_stream_router() — SSE
  tests/market/
    test_models.py  test_cache.py  test_simulator.py
    test_simulator_source.py  test_massive.py  test_factory.py
    test_stream.py          # [NEW]
```

`massive` is a core dependency in `pyproject.toml` (imported at module top level — the review removed the lazy import because it made tests fragile). `numpy` is required by the simulator. Both are installed in the Docker image, so "optional import" buys nothing.

---

## 4. Data Model

**`models.py`** — `PriceUpdate` is the only value that leaves the market layer.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float                     # price at the prior cache update
    timestamp: float = field(default_factory=time.time)   # Unix seconds
    day_open: float | None = None             # [NEW] baseline for daily change %

    @property
    def change(self) -> float:
        """Tick-to-tick absolute change."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Tick-to-tick percent change."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up' | 'down' | 'flat' — drives the green/red flash."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change_percent(self) -> float | None:   # [NEW]
        """Percent change vs. the day baseline (what the watchlist displays)."""
        if not self.day_open:
            return None
        return round((self.price - self.day_open) / self.day_open * 100, 4)

    def to_dict(self) -> dict:
        """Single serialization point for SSE and REST."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "day_open": self.day_open,
            "day_change_percent": self.day_change_percent,
        }
```

### Two kinds of "change"

PLAN.md needs both:

- **Tick change** (`change`, `change_percent`, `direction`): vs. the previous update. Drives the 500 ms flash animation.
- **Day change** (`day_change_percent`): vs. a baseline. Drives the "daily change %" column. `[NEW]` — the current code only has tick change, which would show ~0.00% for most ticks and is not what a trader means by "daily change".

Baseline per source:

| Source | `day_open` |
|---|---|
| Simulator | The seed price (first price the session saw) — "session open" |
| Massive | `snap.day.previous_close` (prior session close), falling back to `snap.day.open`, else first price seen |

Frozen/`slots` dataclass: safe to share across tasks without copying; derived properties cannot drift out of sync with stored fields.

---

## 5. Price Cache

**`cache.py`** — central hub. Sources write; SSE, portfolio and trades read.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory store of the latest PriceUpdate per ticker."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._day_open: dict[str, float] = {}      # [NEW] ticker -> baseline
        self._lock = Lock()
        self._version = 0                          # bumped on every mutation

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        day_open: float | None = None,             # [NEW] source-supplied baseline
    ) -> PriceUpdate:
        """Record a price. First update for a ticker is 'flat' (previous == price)."""
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            if day_open is not None:
                self._day_open[ticker] = round(day_open, 2)
            # setdefault: first-ever price becomes the baseline if no source gave one
            baseline = self._day_open.setdefault(ticker, round(price, 2))

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=timestamp or time.time(),
                day_open=baseline,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow-copy snapshot (PriceUpdates are immutable, so this is safe)."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)
            self._day_open.pop(ticker, None)
            self._version += 1                     # [NEW] removal is a change too

    @property
    def version(self) -> int:
        with self._lock:                           # [NEW] review item 3.4
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Notes:

- **Version counter** lets the SSE loop skip emitting when nothing changed (Massive only changes every 15 s; without it we would re-send identical 10-ticker payloads 30× per poll). `remove()` now bumps it so clients learn a ticker disappeared `[NEW]`.
- **Prices rounded to 2 dp** at the cache boundary, so every consumer (trades, valuation, UI) sees identical numbers regardless of source.
- **Memory is O(tickers)**: only the latest value is kept. Sparkline history is the frontend's job (PLAN.md §2).

---

## 6. Unified Interface

**`interface.py`**

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push into a shared PriceCache on their own schedule.
    Callers never ask a source for a price; they read the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates. Call once. Seeds the cache before returning
        where the source can (simulator: always; Massive: after first poll)."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel background work. Idempotent."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and evict it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

Contract details every implementation must honor:

1. **Normalize** tickers (`upper().strip()`) at the boundary. `[NEW]` — the current simulator doesn't normalize; the Massive client does. Do it in both.
2. `add_ticker` of an existing ticker and `remove_ticker` of a missing one are silent no-ops.
3. `stop()` can be called twice.
4. A single bad tick/poll must never kill the background task.

---

## 7. Seed Data

**`seed_prices.py`** — constants only.

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00,    "NFLX": 600.00,
}

# sigma = annualized volatility, mu = annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # most volatile
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # volatile, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # bank, calm
    "V":     {"sigma": 0.17, "mu": 0.04},   # payments, calmest
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}        # dynamically added tickers

CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR    = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR   = 0.3     # also the fallback for unknown tickers and TSLA
TSLA_CORR          = 0.3     # TSLA "does its own thing"
```

`DEFAULT_CORR` from the first design is deleted (review 4.3): it duplicated `CROSS_GROUP_CORR` and was never read.

---

## 8. Simulator

**`simulator.py`** has two classes: `GBMSimulator` (pure math, synchronous, trivially unit-testable) and `SimulatorDataSource` (async wrapper implementing the interface).

### 8.1 The math

Each tick, for each ticker:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

- `dt = 0.5 s / (252 days · 6.5 h · 3600 s) ≈ 8.48e-8` of a trading year.
- `Z` are *correlated* N(0,1) draws: `Z = L · ε`, where `ε` ~ N(0, I) and `L = cholesky(C)` for the correlation matrix `C`.
- After the GBM step, with probability `0.001` per ticker per tick, apply a shock of ±(2–5)%. With 10 tickers at 2 ticks/s that is an event roughly every 50 s.
- `exp()` guarantees prices stay positive.

Sanity check on scale: for TSLA (σ = 0.5), per-tick σ·√dt ≈ 0.5 · 2.9e-4 ≈ 1.5e-4 → ~$0.04 on $250. Over a simulated 6.5 h day (46,800 ticks) the standard deviation is ≈ 0.5·√(1/252) ≈ 3.1%, which is realistic for a volatile name.

### 8.2 `GBMSimulator`

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
    INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600      # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR      # ~8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for t in tickers:                       # batch init: build Cholesky once
            self._add_ticker_internal(t)
        self._rebuild_cholesky()

    # ---- public -------------------------------------------------------

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z                       # correlate

        out: dict[str, float] = {}
        sqrt_dt = math.sqrt(self._dt)
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            self._prices[ticker] *= math.exp(
                (mu - 0.5 * sigma**2) * self._dt + sigma * sqrt_dt * z[i]
            )

            if random.random() < self._event_prob:       # drama
                shock = random.uniform(0.02, 0.05) * random.choice((-1, 1))
                self._prices[ticker] *= 1 + shock
                logger.debug("Event on %s: %+.1f%%", ticker, shock * 100)

            out[ticker] = round(self._prices[ticker], 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:                  # review 3.5: no private access
        return list(self._tickers)

    # ---- internals ----------------------------------------------------

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech, fin = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in fin and t2 in fin:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

**Why Cholesky is always safe here.** The matrix has 1s on the diagonal and off-diagonals in {0.3, 0.5, 0.6}. Any such matrix with all off-diagonals ≤ 0.6 and ≥ 0.3 is diagonally-dominated enough to be positive definite for n up to ~50 in practice (e.g. the worst case is all-0.6: eigenvalues are `1 + (n−1)·0.6` and `0.4`, both > 0). Mixed groupings are a block structure of the same kind. A test over the full default set (§15) guards this.

### 8.3 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        tickers = [t.upper().strip() for t in tickers]               # [NEW] normalize
        self._sim = GBMSimulator(tickers, event_probability=self._event_prob)
        # Seed immediately so the first SSE frame is never empty. The seed
        # price is also the day baseline for daily-change %.
        for t in tickers:
            p = self._sim.get_price(t)
            if p is not None:
                self._cache.update(t, p, day_open=p)                 # [NEW] day_open
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if not self._sim or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        p = self._sim.get_price(ticker)
        if p is not None:
            self._cache.update(ticker, p, day_open=p)    # price available instantly

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for t, p in self._sim.step().items():
                        self._cache.update(t, p)
            except Exception:
                logger.exception("Simulator step failed")   # keep ticking
            await asyncio.sleep(self._interval)
```

Behaviors worth noting:

- `step()` is ~microseconds for 10 tickers, so it runs inline on the loop (no thread needed).
- Between `start()` and the first loop tick the cache already holds seed prices → no blank frame.
- `asyncio.sleep(interval)` after the step gives ~500 ms + step time (drift of microseconds; irrelevant).
- Adding a ticker rebuilds an n×n Cholesky inline: O(n³) for n ≈ 10–50 is well under 1 ms.

---

## 9. Massive Client

**`massive_client.py`** — polls the Polygon.io-compatible snapshot endpoint (`GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=…`) once per interval for **all** tracked tickers, in a single call. Reference: `planning/archive/MASSIVE_API.md`.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache,
                 poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = [t.upper().strip() for t in tickers]
        await self._poll_once()                       # prime the cache now
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker and ticker not in self._tickers:
            self._tickers.append(ticker)
            # [NEW] don't wait up to 15 s for the next poll: fetch now so the
            # UI / a trade right after "add" has a price.
            await self._poll_once()

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---- internals ----------------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous → keep the event loop free.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
        except Exception as e:                         # 401 / 403 / 429 / network
            logger.error("Massive poll failed: %s", e)
            return                                     # keep last-known prices

        seen = 0
        for snap in snapshots:
            try:
                self._cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000.0,   # ms → s
                    day_open=self._baseline(snap),                  # [NEW]
                )
                seen += 1
            except (AttributeError, TypeError) as e:
                logger.warning("Skipping snapshot for %s: %s",
                               getattr(snap, "ticker", "???"), e)
        logger.debug("Massive poll: %d/%d tickers", seen, len(self._tickers))

    @staticmethod
    def _baseline(snap) -> float | None:
        """Prior close if provided, else today's open, else None (cache falls
        back to the first price it sees)."""
        day = getattr(snap, "day", None)
        for attr in ("previous_close", "open"):
            value = getattr(day, attr, None) if day else None
            if value:
                return value
        return None

    def _fetch_snapshots(self) -> list:
        """Blocking call; always runs inside asyncio.to_thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Rate-limit budget

| Tier | Limit | Configured interval | Calls/min |
|---|---|---|---|
| Free | 5 req/min | 15 s (default) | 4 |
| Paid | high | 2–5 s | 12–30 |

`[NEW]` caveat: `add_ticker` triggers an extra immediate poll. On the free tier a user adding 3 tickers in rapid succession would burn 3 extra calls and trip 429. Guard it with a minimum spacing:

```python
MIN_POLL_SPACING = 3.0   # seconds; set to poll_interval/5 or similar per tier

async def _poll_once(self) -> None:
    now = time.monotonic()
    if now - self._last_poll < self._min_spacing:
        return                         # the regular loop will pick it up
    self._last_poll = now
    ...
```

(`self._last_poll = 0.0` and `self._min_spacing = min(3.0, interval / 5)` in `__init__`.)

### Failure matrix

| Condition | Behavior |
|---|---|
| 401 bad key | Logged each poll; cache stays empty; SSE sends nothing; fix `.env`, restart |
| 403 plan lacks endpoint | Same as 401 |
| 429 rate limit | Logged; retry next interval; cache keeps last prices |
| Network timeout / 5xx | `RESTClient` retries internally; then logged; retry next interval |
| One malformed snapshot | That ticker skipped with a warning; the rest update |
| Ticker not returned (invalid symbol) | Never enters cache; trade attempts get "price not available" (§14) |
| Market closed | `last_trade.price` = last trade (may be after-hours); cache timestamp is old but valid |

### Field mapping

| Massive field | Used for |
|---|---|
| `snap.ticker` | cache key |
| `snap.last_trade.price` | `PriceUpdate.price` |
| `snap.last_trade.timestamp` (ms) | `PriceUpdate.timestamp` (÷1000 → s) |
| `snap.day.previous_close` | `day_open` baseline (daily change %) |
| `snap.day.open` | baseline fallback |

Unused but available if later wanted: `last_quote.bid_price/ask_price`, `day.volume`, `day.change_percent`.

Between polls, Massive prices are static; the SSE version check means no frames are sent until the next poll. The frontend's "price flash" fires only when a new value arrives, which is correct for 15 s data.

---

## 10. Factory

**`factory.py`**

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → Massive; otherwise → simulator.
    Returns an unstarted source; caller must `await source.start(tickers)`."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

Optional env overrides `[NEW]` (all have sensible defaults; omit unless needed):

```python
interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))
return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)
```

A paid-tier user sets `MASSIVE_POLL_INTERVAL=2`.

---

## 11. SSE Stream

**`stream.py`** — `GET /api/stream/prices`.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    # [NEW] router created per call, not module-level (review 3.6): calling this
    # twice — e.g. one app per test — no longer double-registers the route.
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"                    # browser reconnects after 1 s

    last_version = -1
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                if prices:
                    payload = {t: u.to_dict() for t, u in prices.items()}
                    yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
```

### Wire format

One event = one JSON object keyed by ticker, containing **all** currently known tickers:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.52,"previous_price":190.49,"timestamp":1760000000.5,"change":0.03,"change_percent":0.0157,"direction":"up","day_open":190.0,"day_change_percent":0.2737},"GOOGL":{...}}

```

All tickers in every frame (rather than only the changed ones) keeps the client trivial and makes reconnects self-healing: the first frame after a reconnect is a complete snapshot.

### Frontend contract

```ts
type PriceUpdate = {
  ticker: string; price: number; previous_price: number; timestamp: number;
  change: number; change_percent: number; direction: "up" | "down" | "flat";
  day_open: number | null; day_change_percent: number | null;
};

const es = new EventSource("/api/stream/prices");
es.onopen    = () => setStatus("connected");        // green dot
es.onerror   = () => setStatus("reconnecting");     // yellow; EventSource retries itself
es.onmessage = (e) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(e.data);
  for (const p of Object.values(prices)) {
    sparkline[p.ticker].push(p.price);               // accumulate since page load
    flash(p.ticker, p.direction);                    // CSS class, removed after ~500 ms
  }
};
```

### Why poll-and-push

The generator samples the cache on a fixed 500 ms cadence rather than reacting to writes. Frames are evenly spaced (clean sparklines), the generator needs no subscription plumbing, and N clients cost N cheap loops reading the same immutable snapshots. The version check removes the cost of idle frames.

---

## 12. App Integration

**`backend/app/main.py`** (the market-data parts). Startup order matters: the cache and source must exist before the SSE router serves its first request.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = PriceCache()
    source = create_market_data_source(cache)
    app.state.price_cache = cache
    app.state.market_source = source

    # Track watchlist ∪ open positions so valuation stays correct (§13).
    tickers = sorted(set(await db.watchlist_tickers()) | set(await db.position_tickers()))
    await source.start(tickers)

    yield

    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
```

Router registration must happen at import time, not inside `lifespan` (FastAPI builds the route table before startup). Because `create_stream_router` needs the cache, build the cache at module level and share it:

```python
price_cache = PriceCache()                      # module-level singleton
app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# lifespan() then uses this same price_cache instead of creating a new one.
```

(The first design registered the router inside `lifespan`, which works in newer Starlette but is fragile; the module-level cache is simpler and explicit. Static-file mounting for the Next.js export must come **last**, after all `/api` routes, since `app.mount("/", StaticFiles(...))` matches everything.)

Dependencies for REST handlers:

```python
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache

def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

---

## 13. Watchlist & Position Coordination

The source's tracked set must be **watchlist ∪ tickers with open positions**. Otherwise un-watching a held stock would freeze portfolio valuation and make the position unsellable ("no price").

Put this in one service function instead of scattering it across the REST routes and the LLM action executor — both must behave identically:

```python
# backend/app/services/watchlist.py
async def add_to_watchlist(ticker: str, source: MarketDataSource, cache: PriceCache) -> dict:
    ticker = normalize(ticker)                       # upper, strip, ^[A-Z.]{1,6}$
    await db.insert_watchlist(ticker)                # idempotent (UNIQUE user_id,ticker)
    await source.add_ticker(ticker)
    price = cache.get_price(ticker)
    return {"ticker": ticker, "price": price}        # price may be None briefly on Massive


async def remove_from_watchlist(ticker: str, source: MarketDataSource) -> None:
    ticker = normalize(ticker)
    await db.delete_watchlist(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:   # still held → keep streaming it
        await source.remove_ticker(ticker)
```

Reverse direction: when a **buy** opens a position in a ticker that isn't watched (possible via the trade bar), call `await source.add_ticker(ticker)` first, which also guarantees a price exists. When a **sell** closes a position and the ticker isn't on the watchlist, call `source.remove_ticker(ticker)`.

```
POST /api/watchlist {PYPL}
  → INSERT watchlist → source.add_ticker  (sim: seed+cache now; massive: poll now)
  → 200 {ticker, price|null}

DELETE /api/watchlist/PYPL
  → DELETE watchlist → (no position?) source.remove_ticker → cache evicts → next SSE frame omits it
```

The frontend should treat the SSE stream as the source of truth for *prices* and `/api/watchlist` as the source of truth for *membership*; render the intersection. Held-but-unwatched tickers appear in SSE but not in the watchlist panel (they appear in the positions table).

---

## 14. Error Handling & Edge Cases

| Case | Handling |
|---|---|
| Empty watchlist at startup | `start([])` is valid for both sources; SSE sends nothing until a ticker exists |
| Trade on a ticker with no cached price | `HTTP 400 "Price not yet available for XYZ"`; LLM path surfaces the same message in the chat reply |
| Unknown ticker, simulator | Accepted; gets a random $50–$300 seed and default params. Acceptable for a demo (and lets the LLM "add PYPL") |
| Unknown ticker, Massive | Accepted into the tracked list but never gets a price. `[NEW]` After an `add_ticker`-triggered poll, the watchlist route should check `cache.get_price()`; if still `None` on a real-data source, reply `404 "Unknown ticker"` and roll back the DB insert |
| Duplicate add / missing remove | No-ops at all layers |
| Bad Massive key | Per-poll error log; no prices; app still serves UI. A one-line startup warning if the first poll yields zero tickers is recommended `[NEW]` |
| Simulator exception in a step | Logged, loop continues |
| Container restart | Cache is rebuilt from seeds / first poll; sparklines restart (by design) |
| Prices after market close (Massive) | Static last-trade; flash animations simply don't fire |
| Thread safety | One mutex over tiny critical sections; contention negligible at ≤50 tickers |
| Precision | Prices rounded to cents in the cache; trade quantities are `REAL` per schema, so P&L uses the rounded cache price |

---

## 15. Testing

Run with `cd backend && uv run pytest tests/market -v` (asyncio mode is `auto` in `pyproject.toml`; no `@pytest.mark.asyncio` needed).

### Existing suite (73 tests) — keep

`test_models`, `test_cache`, `test_simulator`, `test_simulator_source`, `test_factory`, `test_massive`.

### New / changed tests `[NEW]`

```python
# test_cache.py
def test_day_open_defaults_to_first_price():
    c = PriceCache()
    c.update("AAPL", 190.0)
    u = c.update("AAPL", 195.0)
    assert u.day_open == 190.0
    assert u.day_change_percent == pytest.approx(2.6316, abs=1e-3)

def test_explicit_day_open_overrides():
    c = PriceCache()
    u = c.update("AAPL", 195.0, day_open=190.0)
    assert u.day_open == 190.0

def test_remove_bumps_version_and_clears_baseline():
    c = PriceCache(); c.update("AAPL", 190.0); v = c.version
    c.remove("AAPL")
    assert c.version == v + 1
    assert c.update("AAPL", 200.0).day_open == 200.0     # baseline reset

def test_concurrent_writers():
    import threading
    c = PriceCache()
    def work(sym):
        for i in range(1000):
            c.update(sym, 100 + i)
    ts = [threading.Thread(target=work, args=(f"T{i}",)) for i in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert len(c) == 8 and c.version == 8000
```

```python
# test_simulator.py
from app.market.seed_prices import SEED_PRICES

def test_cholesky_succeeds_for_default_set():
    sim = GBMSimulator(tickers=list(SEED_PRICES))
    assert sim._cholesky is not None and sim._cholesky.shape == (10, 10)

def test_correlation_is_realized():
    """Tech names should co-move more than tech vs. finance."""
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0)
    rets = {t: [] for t in ("AAPL", "MSFT", "JPM")}
    prev = {t: sim.get_price(t) for t in rets}
    for _ in range(5000):
        for t, p in sim.step().items():
            rets[t].append(p / prev[t] - 1); prev[t] = p
    c = np.corrcoef([rets["AAPL"], rets["MSFT"], rets["JPM"]])
    assert c[0, 1] > c[0, 2]

def test_shock_events_fire():
    sim = GBMSimulator(["AAPL"], event_probability=1.0)   # always shock
    p0 = sim.get_price("AAPL")
    assert abs(sim.step()["AAPL"] / p0 - 1) >= 0.019      # ≥2% (minus GBM drift)
```

```python
# test_simulator_source.py
async def test_tickers_are_normalized():
    cache = PriceCache(); s = SimulatorDataSource(cache, update_interval=0.05)
    await s.start(["aapl"]); await s.add_ticker(" tsla ")
    assert set(s.get_tickers()) == {"AAPL", "TSLA"}
    await s.stop()

async def test_loop_survives_step_error(monkeypatch):
    cache = PriceCache(); s = SimulatorDataSource(cache, update_interval=0.01)
    await s.start(["AAPL"])
    calls = {"n": 0}
    real = s._sim.step
    def flaky():
        calls["n"] += 1
        if calls["n"] == 1: raise RuntimeError("boom")
        return real()
    monkeypatch.setattr(s._sim, "step", flaky)
    await asyncio.sleep(0.1)
    assert calls["n"] > 1                      # kept going after the exception
    await s.stop()
```

```python
# test_massive.py — make robust without a live API
def _snap(ticker, price, ts_ms, prev_close=None):
    s = MagicMock(); s.ticker = ticker
    s.last_trade.price = price; s.last_trade.timestamp = ts_ms
    s.day.previous_close = prev_close; s.day.open = None
    return s

async def test_day_open_from_previous_close():
    cache = PriceCache()
    src = MassiveDataSource("k", cache); src._tickers = ["AAPL"]; src._client = object()
    with patch.object(src, "_fetch_snapshots", return_value=[_snap("AAPL", 195, 1_700_000_000_000, 190.0)]):
        await src._poll_once()
    u = cache.get("AAPL")
    assert u.day_open == 190.0 and u.timestamp == 1_700_000_000.0

async def test_remove_evicts_from_cache():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    src = MassiveDataSource("k", cache); src._tickers = ["AAPL"]
    await src.remove_ticker("aapl")
    assert cache.get("AAPL") is None and src.get_tickers() == []
```

```python
# test_stream.py — SSE with an ASGI client
import json, httpx
from fastapi import FastAPI
from app.market import PriceCache, create_stream_router

async def test_sse_emits_snapshot_and_headers():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    app = FastAPI(); app.include_router(create_stream_router(cache))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        async with c.stream("GET", "/api/stream/prices") as r:
            assert r.headers["content-type"].startswith("text/event-stream")
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    body = json.loads(line[5:])
                    assert body["AAPL"]["price"] == 190.0
                    break

def test_router_factory_is_repeatable():
    c = PriceCache()
    a, b = create_stream_router(c), create_stream_router(c)
    assert len(a.routes) == len(b.routes) == 1     # fails with a module-level router
```

Add `httpx` to the dev dependencies for the SSE test. Note `ASGITransport` buffers streaming bodies in some httpx versions; if the test hangs, run the generator directly with a stub `Request` whose `is_disconnected()` flips to `True` after the first yield.

### E2E (PLAN.md §12)

With `LLM_MOCK=true` and no `MASSIVE_API_KEY` (simulator): assert ten rows with changing prices, a green connection dot, and reconnection after the server connection is dropped.

---

## 16. Configuration Reference

| Parameter | Where | Default | Meaning |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | `""` | non-empty → Massive; empty → simulator |
| `MASSIVE_POLL_INTERVAL` `[NEW]` | env | `15` | seconds between Massive polls (free tier: ≥15) |
| `update_interval` | `SimulatorDataSource` | `0.5` | simulator tick (s) |
| `event_probability` | `GBMSimulator` | `0.001` | shock chance per ticker per tick |
| `dt` | `GBMSimulator` | `≈8.48e-8` | step as fraction of a trading year |
| SSE `interval` | `_generate_events` | `0.5` | seconds between cache samples |
| SSE `retry` | stream preamble | `1000` | ms before browser reconnect |
| `MIN_POLL_SPACING` `[NEW]` | `MassiveDataSource` | `min(3, interval/5)` | throttle for on-demand polls |

Public API of the package (`app/market/__init__.py`):

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = ["PriceUpdate", "PriceCache", "MarketDataSource",
           "create_market_data_source", "create_stream_router"]
```

---

## 17. Worked Examples

### A. Startup and first SSE frame (simulator)

```python
cache  = PriceCache()
source = create_market_data_source(cache)          # no MASSIVE_API_KEY → simulator
await source.start(["AAPL", "GOOGL", "MSFT"])

cache.get("AAPL")
# PriceUpdate(ticker='AAPL', price=190.0, previous_price=190.0, ..., day_open=190.0)
# direction == 'flat'   (first sample)

await asyncio.sleep(0.6)                           # one tick has run
u = cache.get("AAPL")
# u.price ~ 190.03, u.previous_price == 190.0, u.direction == 'up',
# u.day_change_percent ~ 0.0158
```

### B. Executing a trade against the cache

```python
async def execute_trade(ticker: str, side: str, qty: float, cache: PriceCache, source):
    ticker = ticker.upper().strip()
    price = cache.get_price(ticker)
    if price is None:
        await source.add_ticker(ticker)            # make sure it is tracked
        price = cache.get_price(ticker)
        if price is None:
            raise HTTPException(400, f"Price not yet available for {ticker}")
    # fill at `price`; update cash/positions/trades; record portfolio snapshot ...
```

### C. Portfolio valuation

```python
def portfolio_value(cash: float, positions: list[Position], cache: PriceCache) -> float:
    total = cash
    for p in positions:
        price = cache.get_price(p.ticker) or p.avg_cost   # fall back if feed hiccups
        total += p.quantity * price
    return round(total, 2)
```

### D. Switching to real data

```bash
# .env
MASSIVE_API_KEY=your-key
MASSIVE_POLL_INTERVAL=15      # free tier; use 2–5 on paid plans
```

No code changes: `create_market_data_source()` returns `MassiveDataSource`; the SSE stream, positions table, trades and heatmap behave identically, except that frames arrive every poll interval instead of every 500 ms and `day_open` comes from the real previous close.

### E. What one Massive poll looks like

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
→ [{ticker: "AAPL", day:{previous_close:189.7, open:190.1, ...},
     last_trade:{price:191.23, timestamp:1760000000123}}, ...]

cache.update("AAPL", 191.23, timestamp=1760000000.123, day_open=189.7)
  → PriceUpdate(price=191.23, previous_price=<last poll's price>, day_open=189.7)
  → version += 1  → next SSE tick emits one frame with all tickers
```

### F. Tuning the simulator for a demo

```python
SimulatorDataSource(cache, update_interval=0.25)              # livelier ticks
SimulatorDataSource(cache, event_probability=0.005)           # a shock every ~10 s
GBMSimulator(tickers, dt=GBMSimulator.DEFAULT_DT * 20)        # 20× more volatile paths for screenshots
```

---

## Change Summary vs. Current Code (`[NEW]` items, in priority order)

1. **Day baseline** — `PriceUpdate.day_open` / `day_change_percent`, `PriceCache.update(day_open=…)`, simulator seeds it, Massive reads `day.previous_close`. Required for the watchlist's "daily change %".
2. **Positions-aware tracking** — track watchlist ∪ positions (§13), via a shared watchlist service used by REST and the LLM executor.
3. **Per-call SSE router** — remove the module-level `router` in `stream.py`.
4. **`remove()` bumps version; `version` read under lock.**
5. **Ticker normalization in `SimulatorDataSource`.**
6. **Massive `add_ticker` immediate (throttled) poll**, and unknown-ticker detection on real data.
7. **`MASSIVE_POLL_INTERVAL` env override.**
8. **New tests**: day baseline, concurrency, correlation, shocks, loop resilience, SSE, router idempotence.
