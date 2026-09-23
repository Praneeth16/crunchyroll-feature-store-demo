"""The homepage must render without a fresh ranking. This is how.

docs/open_items.md #5: past its provisioned concurrency the rail endpoint sheds load
with HTTP 429 rather than queueing (8,770 of 13,988 requests in the spike test), and it
does not scale up in seconds. So every model call here has a budget, a circuit breaker,
and a defined answer when either trips:

  rails   model  ->  last good ranking for this viewer  ->  editorial order
  titles  model  ->  retrieval order                     ->  popularity order

The last-good ranking is filtered to the viewer's CURRENT eligible set -- a cached
order that still shows "Continue Watching" after it stopped being eligible would be a
fallback that is wrong rather than stale.

Every response carries which tier served it. A fallback that is invisible in the
response is one nobody measures.
"""
import asyncio
import time
from collections import OrderedDict

from . import settings as S


class Breaker:
    """Closed -> open after N consecutive failures -> half-open after a cooldown,
    where one trial call decides. Timeouts, 429s and 5xx all count."""

    def __init__(self, name: str):
        self.name = name
        self.failures = 0
        self.opened_at = None
        self.trips = 0

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        if time.time() - self.opened_at >= S.BREAKER_COOLDOWN_S:
            return "half_open"
        return "open"

    def ok(self):
        self.failures, self.opened_at = 0, None

    def fail(self):
        self.failures += 1
        if self.state == "half_open" or self.failures >= S.BREAKER_FAILURES:
            if self.opened_at is None or self.state == "half_open":
                self.trips += 1
            self.opened_at = time.time()

    def snapshot(self) -> dict:
        return {"state": self.state, "consecutive_failures": self.failures, "trips": self.trips}


breakers = {k: Breaker(k) for k in ("rails", "titles", "retriever")}

# Demo control: "off" | "timeout" (every call behaves as if it blew its budget, so the
# breaker trips for real) | "open" (breakers forced open). Process-local, on purpose.
simulate = {"mode": "off"}


async def guarded(name: str, timeout_ms: int, coro_fn):
    """Run coro_fn() under a budget and a breaker. Returns (result_or_None, info)."""
    br = breakers[name]
    t0 = time.perf_counter()
    info = {"budget_ms": timeout_ms}
    if simulate["mode"] == "open" or br.state == "open":
        return None, {**info, "status": "breaker_open", "ms": 0.0}
    try:
        if simulate["mode"] == "timeout":
            await asyncio.sleep(timeout_ms / 1000.0)
            raise asyncio.TimeoutError
        result = await asyncio.wait_for(coro_fn(), timeout_ms / 1000.0)
        br.ok()
        return result, {**info, "status": "ok", "ms": _ms(t0)}
    except asyncio.TimeoutError:
        br.fail()
        return None, {**info, "status": "timeout", "ms": _ms(t0)}
    except Exception as e:
        br.fail()
        status = getattr(e, "status", None)
        return None, {**info, "status": f"http_{status}" if status else "error",
                      "error": str(e)[:200], "ms": _ms(t0)}


def _ms(t0):
    return round((time.perf_counter() - t0) * 1000.0, 1)


class LastGood:
    """Per-viewer last successful rail order, LRU-bounded."""

    def __init__(self, capacity: int = 10_000):
        self.capacity = capacity
        self._d: OrderedDict = OrderedDict()

    def put(self, viewer: str, rail_ids: list):
        self._d[viewer] = (rail_ids, time.time())
        self._d.move_to_end(viewer)
        while len(self._d) > self.capacity:
            self._d.popitem(last=False)

    def get(self, viewer: str):
        hit = self._d.get(viewer)
        if hit:
            self._d.move_to_end(viewer)
        return hit


last_good = LastGood()


def fallback_rails(viewer: str, eligible: list[dict]) -> tuple[list[str], str, float | None]:
    """Order for `eligible` without the model. Returns (rail_ids, source, age_s)."""
    editorial = [r["rail_id"] for r in sorted(eligible, key=lambda r: r["editorial_rank"])]
    hit = last_good.get(viewer)
    if hit:
        order, at = hit
        allowed = set(editorial)
        kept = [r for r in order if r in allowed]
        kept += [r for r in editorial if r not in set(kept)]
        return kept, "cached", round(time.time() - at, 1)
    return editorial, "editorial", None
