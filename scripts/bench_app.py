"""Measure the deployed homepage service: server-side time per stage, as the app reports it.

    python3 scripts/bench_app.py --profile <PROFILE> [--n 60]

`total_ms` is measured inside the app container, so it is the in-region number the
homepage would see. The laptop round trip is printed too, for contrast: it adds the
Apps proxy and the distance to us-west-2, neither of which is the service's cost.
"""
import argparse
import collections
import json
import subprocess
import time
import urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--app", default="crfs-watch-next")
ap.add_argument("--n", type=int, default=60)
a = ap.parse_args()

db = lambda *args: json.loads(subprocess.check_output(["databricks", *args, "--profile", a.profile, "-o", "json"]))
url = db("apps", "get", a.app)["url"]
tok = json.loads(subprocess.check_output(["databricks", "auth", "token", "--profile", a.profile]))["access_token"]


def call(viewer, device, hour):
    req = urllib.request.Request(
        f"{url}/api/homepage", data=json.dumps({"viewer_id": viewer, "device": device, "hour": hour}).encode(),
        headers={"Authorization": f"Bearer {tok}", "content-type": "application/json"})
    t0 = time.perf_counter()
    body = json.load(urllib.request.urlopen(req, timeout=30))
    return body, (time.perf_counter() - t0) * 1000


pct = lambda xs, q: sorted(xs)[min(len(xs) - 1, int(q * len(xs)))]
for v in ("v0001", "v0002"):
    call(v, "tv", 21)
total, rt, stages, sources = [], [], collections.defaultdict(list), collections.Counter()
for i in range(a.n):
    # Each viewer twice, in different contexts: the second view is the common case
    # (same person, new device or hour) and is where per-viewer caching shows up.
    d, ms = call(f"v{(i // 2) % 300 + 1:04d}", ("tv", "mobile")[i % 2], (21, 9)[i % 2])
    total.append(d["total_ms"])
    rt.append(ms)
    for s in d["timings"]:
        stages[s["stage"]].append(s["ms"])
    sources[f'rails={d["rails"]["source"]} titles={d["titles"]["source"]}'] += 1

print(f"{a.n} requests against {url}")
print(f"server total   p50 {pct(total, .5):6.0f}  p95 {pct(total, .95):6.0f}  max {max(total):6.0f} ms")
print(f"laptop RT      p50 {pct(rt, .5):6.0f}  p95 {pct(rt, .95):6.0f} ms")
for k, v in stages.items():
    print(f"  {k:22s} p50 {pct(v, .5):6.1f}  p95 {pct(v, .95):6.1f}")
print("served by:", dict(sources))
