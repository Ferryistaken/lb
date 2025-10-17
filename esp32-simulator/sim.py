#!/usr/bin/env python3
"""
ESP32-like HTTP server simulator for load balancer testing.

Usage:
  python simulator.py 5                # start 5 servers on 127.0.0.1:8080..8084
  python simulator.py 3 --host 0.0.0.0 --base-port 9000
  python simulator.py 2 --mdns        # advertise via mDNS (_http._tcp) if zeroconf is installed
"""

import argparse
import asyncio
import contextlib
import socket
import sys
import time
from typing import List, Optional

try:
    from aiohttp import web
except ImportError:
    print("This script requires aiohttp. Install with:  pip install aiohttp", file=sys.stderr)
    sys.exit(1)

# mDNS (optional)
try:
    from zeroconf import IPVersion, ServiceInfo, Zeroconf  # type: ignore
    HAS_ZEROCONF = True
except Exception:
    HAS_ZEROCONF = False


def pick_primary_ip(preferred_host: str) -> str:
    """
    Best-effort way to figure out the local IP that would be used for outbound traffic.
    If preferred_host is not 0.0.0.0, just return it.
    """
    if preferred_host and preferred_host != "0.0.0.0":
        return preferred_host
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Doesn't actually send packets; just picks routing table path
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


async def make_app(instance_name: str):
    app = web.Application()

    async def handle_root(request: web.Request) -> web.StreamResponse:
        # “LED flash” (log) immediately on request
        now_ms = int(time.time() * 1000)
        print(f"[{instance_name}] LED FLASH at {now_ms} ms — request from {request.remote}")

        # mark time at handler entry (approx “received”)
        start_us = time.time_ns() // 1000

        # Use StreamResponse so we can send headers first, then compute & write body
        resp = web.StreamResponse(status=200, reason="OK", headers={"Content-Type": "text/plain"})
        await resp.prepare(request)  # headers flushed here

        # elapsed until we *send* the body
        delta_us = (time.time_ns() // 1000) - start_us
        payload = f"{delta_us} us\n".encode("utf-8")

        await resp.write(payload)
        await resp.write_eof()
        return resp

    async def handle_health(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    app.add_routes([web.get("/", handle_root), web.get("/_healthz", handle_health)])
    return app


class MDNSAdvertiser:
    """Optional mDNS advertiser per instance (zeroconf)."""

    def __init__(self, name: str, host_ip: str, port: int):
        self.name = name
        self.host_ip = host_ip
        self.port = port
        self.zeroconf: Optional[Zeroconf] = None
        self.info: Optional[ServiceInfo] = None

    def start(self):
        if not HAS_ZEROCONF:
            print(f"[{self.name}] mDNS requested but `zeroconf` not installed; skipping.")
            return
        try:
            addr_bytes = socket.inet_aton(self.host_ip)
            self.info = ServiceInfo(
                type_="_http._tcp.local.",
                name=f"{self.name}._http._tcp.local.",
                addresses=[addr_bytes],
                port=self.port,
                properties={b"path": b"/"},
                server=f"{self.name}.local.",
            )
            self.zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
            self.zeroconf.register_service(self.info)
            print(f"[{self.name}] mDNS advertised: http://{self.name}.local/ (IP {self.host_ip}:{self.port})")
        except Exception as e:
            print(f"[{self.name}] mDNS advertise failed: {e}")

    def stop(self):
        if self.zeroconf and self.info:
            with contextlib.suppress(Exception):
                self.zeroconf.unregister_service(self.info)
                self.zeroconf.close()


async def start_instance(idx: int, host: str, port: int, mdns: bool, base_name: str) -> asyncio.AbstractServer:
    name = f"{base_name}-{idx}"
    app = await make_app(name)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port, reuse_address=True)
    await site.start()

    ip = pick_primary_ip(host)
    print(f"[{name}] Listening on http://{ip if host=='0.0.0.0' else host}:{port}")
    advertiser = None
    if mdns:
        advertiser = MDNSAdvertiser(name, ip, port)
        advertiser.start()

    return site._server  # type: ignore[attr-defined]


async def main():
    parser = argparse.ArgumentParser(description="ESP32-like HTTP server simulator")
    parser.add_argument("n", type=int, nargs="?", default=3,
                     help="number of simulated devices (default: 3)")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (use 0.0.0.0 to expose on LAN)")
    parser.add_argument("--base-port", type=int, default=8080, help="first port; instances use base, base+1, ...")
    parser.add_argument("--mdns", default=False, action="store_true", help="advertise via mDNS (_http._tcp) using zeroconf")
    parser.add_argument("--name-prefix", default="esp32-sim", help="instance name prefix for logs/mDNS")
    args = parser.parse_args()

    if args.mdns and not HAS_ZEROCONF:
        print("⚠️  --mdns requested but `zeroconf` is not installed. Install with:  pip install zeroconf", file=sys.stderr)

    servers: List[asyncio.AbstractServer] = []
    try:
        # Start N servers
        for i in range(args.n):
            srv = await start_instance(i, args.host, args.base_port + i, args.mdns, args.name_prefix)
            servers.append(srv)

        print("\nReady. Test with, e.g.:")
        print(f"  curl http://{pick_primary_ip(args.host)}:{args.base_port}/")
        print("Each response body is the microseconds between handler entry and body send.\nPress Ctrl+C to stop.\n")

        # Run forever
        while True:
            await asyncio.sleep(3600)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Graceful shutdown
        for srv in servers:
            with contextlib.suppress(Exception):
                srv.close()
                await srv.wait_closed()

        # Stop any mDNS advertisements
        # (We attached advertiser to app, but here we don’t keep the app refs; fine for a simulator.)
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        # If already inside an event loop (e.g., in notebooks), fall back
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())

