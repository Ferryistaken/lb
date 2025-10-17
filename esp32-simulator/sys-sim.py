#!/usr/bin/env python3
"""
Simple async load test for the esp32 lb/proxy.

Examples:
  # Just load the proxy at 3001 for 15s at 200 rps
  python bench.py --duration 15 --rps 200 --target http://127.0.0.1:3001/

  # Spin up 3 simulators on 8080..8082, then hit the proxy for 30s @ 500 rps
  python bench.py --duration 30 --rps 500 --n 3 --host 127.0.0.1 --base-port 8080 --target http://127.0.0.1:3001/

  # Show live progress each second
  python bench.py --duration 10 --rps 100 --progress
"""

import argparse
import asyncio
import contextlib
import math
import random
import statistics
import sys
import time
from typing import List, Optional, Tuple

try:
    import aiohttp
except ImportError:
    print("This script requires aiohttp. Install with:  pip install aiohttp", file=sys.stderr)
    sys.exit(1)

# Reuse parts of your simulator (same folder)
try:
    import simulator  # your provided file
except Exception as e:
    simulator = None
    SIM_IMPORT_ERR = e
else:
    SIM_IMPORT_ERR = None


# ---------- Helpers ----------
def pct(vals: List[float], p: float) -> float:
    """percentile with linear interpolation; vals in ms."""
    if not vals:
        return float("nan")
    k = (len(vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return vals[int(k)]
    d0 = vals[f] * (c - k)
    d1 = vals[c] * (k - f)
    return d0 + d1

def human(n: float) -> str:
    return f"{n:,.0f}"


# ---------- Simulator spin-up (optional) ----------
async def start_simulators(n: int, host: str, base_port: int, mdns: bool, name_prefix: str):
    """
    Start N simulator servers within this loop by calling into simulator.py helpers.
    """
    if simulator is None:
        raise RuntimeError(
            f"Couldn't import simulator.py: {SIM_IMPORT_ERR}\n"
            "Place bench.py next to simulator.py (your provided file)."
        )

    servers = []
    for i in range(n):
        srv = await simulator.start_instance(i, host, base_port + i, mdns, name_prefix)
        servers.append(srv)
    return servers


# ---------- Load generator ----------
async def worker(session: aiohttp.ClientSession, target: str, timeout_s: float,
                 results: dict):
    """
    One request; record timing + status. Results dict is shared (single-threaded event loop).
    """
    t0 = time.perf_counter()
    try:
        async with session.get(target, timeout=timeout_s) as resp:
            await resp.read()  # fully consume
            dt_ms = (time.perf_counter() - t0) * 1000.0
            results["lat_ms"].append(dt_ms)
            if resp.ok:
                results["ok"] += 1
            else:
                results["err"] += 1
    except Exception as e:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        results["err"] += 1
        results["err_samples"].append(repr(e))
        results["lat_ms"].append(dt_ms)  # keep timing even on error


async def run_load(duration_s: float, rps: float, target: str,
                   concurrency: Optional[int], timeout_s: float,
                   progress: bool) -> dict:
    """
    Run for `duration_s`, aiming for `rps` requests/sec to `target`.
    If `concurrency` is None, it will float based on RPS pacing.
    """
    results = {
        "ok": 0,
        "err": 0,
        "lat_ms": [],         # list[float]
        "err_samples": [],    # list[str]
        "started": 0,
    }

    # Connector: no limit unless user wants to cap concurrency
    conn = aiohttp.TCPConnector(limit=concurrency or 0, force_close=False)
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    headers = {}  # add x-request-id here if you like

    async with aiohttp.ClientSession(connector=conn, timeout=timeout, headers=headers) as session:
        start = time.perf_counter()
        end = start + duration_s
        interval = 1.0 / rps if rps > 0 else 0.0

        # progress display
        async def show_progress():
            while True:
                await asyncio.sleep(1.0)
                elapsed = time.perf_counter() - start
                if elapsed <= 0:
                    continue
                total = results["ok"] + results["err"]
                cur_rps = total / elapsed
                print(f"[{elapsed:6.2f}s] sent={human(total)} ok={human(results['ok'])} "
                      f"err={human(results['err'])} rps~{cur_rps:,.1f}")
        progress_task = None
        if progress:
            progress_task = asyncio.create_task(show_progress())

        # pacing using a simple scheduler with drift compensation
        next_dispatch = start
        pending: List[asyncio.Task] = []
        try:
            while True:
                now = time.perf_counter()
                if now >= end:
                    break

                # dispatch enough to catch up if we fell behind
                while next_dispatch <= now:
                    results["started"] += 1
                    pending.append(asyncio.create_task(worker(session, target, timeout_s, results)))
                    next_dispatch += interval if interval > 0 else 0.0
                    if interval == 0.0:
                        # If rps <= 0, just fire once and exit the loop.
                        break

                # Small sleep to yield; adjust to your precision needs
                await asyncio.sleep( min(0.001, interval/2 if interval > 0 else 0.001) )

            # Wait for all in-flight
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            if progress_task:
                progress_task.cancel()
                with contextlib.suppress(Exception):
                    await progress_task

    return results

async def fetch_stats(target):
    print(target.split(":"))
    print('\nLoad Balancer Stats:')
    async with aiohttp.ClientSession() as session:
        async with session.options(target) as response:
            stats = await response.text()
            print('\t' + '\n\t'.join(stats.split('\n')))

# ---------- Orchestration ----------
async def orchestrate(args):
    # Optionally start simulators
    servers = []
    if args.n and args.n > 0:
        print(f"Starting {args.n} simulators on {args.host}:{args.base_port}..{args.base_port+args.n-1}")
        servers = await start_simulators(args.n, args.host, args.base_port, args.mdns, args.name_prefix)

    print(f"Load: target={args.target} duration={args.duration}s rps={args.rps} timeout={args.timeout}s")
    if args.concurrency:
        print(f"Connector limit (concurrency cap): {args.concurrency}")
    
    conn = aiohttp.TCPConnector()
    timeout = aiohttp.ClientTimeout()
    headers = {}  # add x-request-id here if you like

    await fetch_stats(args.target)

    t0 = time.perf_counter()
    try:
        results = await run_load(
            duration_s=args.duration,
            rps=args.rps,
            target=args.target,
            concurrency=args.concurrency,
            timeout_s=args.timeout,
            progress=args.progress,
        )
    finally:
        # Stop simulators (graceful)
        for srv in servers:
            with contextlib.suppress(Exception):
                srv.close()
                await srv.wait_closed()

    # ----- Summary -----
    elapsed = time.perf_counter() - t0
    total = results["ok"] + results["err"]
    lat = sorted(results["lat_ms"])
    p50 = pct(lat, 50)
    p95 = pct(lat, 95)
    p99 = pct(lat, 99)
    avg = statistics.mean(lat) if lat else float("nan")

    print("\n=== Summary ===")
    print(f"Duration:     {elapsed:.3f} s   (target {args.duration:.3f} s)")
    print(f"Target:       {args.target}")
    print(f"Sent:         {human(total)}")
    print(f"Success:      {human(results['ok'])}")
    print(f"Errors:       {human(results['err'])}")
    print(f"Achieved RPS: {total/elapsed:,.1f} req/s")
    print(f"Latency ms:   avg={avg:.2f}  p50={p50:.2f}  p95={p95:.2f}  p99={p99:.2f}")

    if results["err"] and results["err_samples"]:
        print("\nSample errors:")
        for s in results["err_samples"][:5]:
            print(f"  - {s}")
    
    await fetch_stats(args.target)


def parse_args(argv: List[str]):
    ap = argparse.ArgumentParser(description="Async load generator for esp32 lb/proxy")
    ap.add_argument("--duration", type=float, default=10.0, help="test duration in seconds (default: 10)")
    ap.add_argument("--rps", type=float, default=100.0, help="requests per second target (default: 100)")
    ap.add_argument("--target", type=str, default="http://127.0.0.1:3001/", help="proxy URL to hit")
    ap.add_argument("--timeout", type=float, default=5.0, help="per-request timeout (seconds)")
    ap.add_argument("--concurrency", type=int, default=None, help="max concurrent connections (aiohttp connector limit). Omit for unlimited.")
    ap.add_argument("--progress", action="store_true", help="print 1s progress updates")

    # Optional simulator spin-up (same options as your simulator)
    ap.add_argument("--n", type=int, default=0, help="number of simulators to start (0 = none)")
    ap.add_argument("--host", type=str, default="127.0.0.1", help="simulator bind host")
    ap.add_argument("--base-port", type=int, default=8080, help="first simulator port")
    ap.add_argument("--mdns", action="store_true", help="advertise simulators via mDNS")
    ap.add_argument("--name-prefix", type=str, default="esp32-sim", help="simulator name prefix")

    return ap.parse_args(argv)


def main():
    args = parse_args(sys.argv[1:])
    try:
        asyncio.run(orchestrate(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")

if __name__ == "__main__":
    main()

