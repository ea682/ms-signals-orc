#!/usr/bin/env python3
"""Loopback-only Hyperliquid /info fixture for packaged HOT certification."""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import parse_qs, urlparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--tid", type=int, required=True)
    parser.add_argument("--source-timestamp", type=int, required=True)
    parser.add_argument("--start-position", default="0")
    parser.add_argument("--coin", default="HYPE")
    parser.add_argument("--price", default="21.25")
    parser.add_argument("--size", default="1")
    parser.add_argument("--side", default="B")
    parser.add_argument("--direction", default="Open Long")
    parser.add_argument("--closed-pnl", default="0")
    parser.add_argument("--fee", default="0.01")
    parser.add_argument("--fills-file")
    args = parser.parse_args()

    wallet = args.wallet.lower()
    if args.fills_file:
        with open(args.fills_file, "r", encoding="utf-8") as stream:
            configured_fills = json.load(stream)
        if not isinstance(configured_fills, list) or not configured_fills:
            raise ValueError("fills file must contain a non-empty JSON array")
        active_tids: set[int] = set()
    else:
        configured_fills = [
            {
                "wallet": wallet,
                "coin": args.coin,
                "px": args.price,
                "sz": args.size,
                "side": args.side,
                "time": args.source_timestamp,
                "startPosition": args.start_position,
                "dir": args.direction,
                "closedPnl": args.closed_pnl,
                "hash": f"0xpackaged{args.tid}",
                "fee": args.fee,
                "tid": args.tid,
                "oid": args.tid,
            }
        ]
        active_tids = {args.tid}

    control_lock = Lock()
    rate_limit_remaining = 0

    def fills_for(request_wallet: str, request: dict) -> list[dict]:
        result = []
        start_time = request.get("startTime")
        end_time = request.get("endTime")
        for configured in configured_fills:
            owner = str(configured.get("wallet", wallet)).lower()
            if owner != request_wallet.lower():
                continue
            fill_time = int(configured.get("time", 0))
            fill_tid = int(configured.get("tid", 0))
            if fill_tid not in active_tids:
                continue
            if start_time is not None and fill_time < int(start_time):
                continue
            if end_time is not None and fill_time > int(end_time):
                continue
            fill = {key: value for key, value in configured.items()
                    if key != "wallet"}
            result.append(fill)
        return result

    universe = sorted({str(fill.get("coin", "HYPE"))
                       for fill in configured_fills})

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - HTTP handler contract
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                with control_lock:
                    remaining = rate_limit_remaining
                self._respond(200, {
                    "status": "UP",
                    "activeFills": len(active_tids),
                    "rateLimitRemaining": remaining,
                })
            else:
                self._respond(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802 - HTTP handler contract
            nonlocal rate_limit_remaining
            parsed = urlparse(self.path)
            if parsed.path == "/activate":
                values = parse_qs(parsed.query).get("tid", [])
                if len(values) != 1:
                    self._respond(400, {"error": "tid_required"})
                    return
                active_tids.add(int(values[0]))
                self._respond(200, {
                    "activated": int(values[0]),
                    "activeFills": len(active_tids),
                })
                return
            if parsed.path == "/rate-limit":
                values = parse_qs(parsed.query).get("count", [])
                if len(values) != 1 or int(values[0]) < 0:
                    self._respond(400, {"error": "non_negative_count_required"})
                    return
                with control_lock:
                    rate_limit_remaining = int(values[0])
                    remaining = rate_limit_remaining
                self._respond(200, {"rateLimitRemaining": remaining})
                return
            if parsed.path != "/info":
                self._respond(404, {"error": "not_found"})
                return
            with control_lock:
                if rate_limit_remaining > 0:
                    rate_limit_remaining -= 1
                    remaining = rate_limit_remaining
                else:
                    remaining = -1
            if remaining >= 0:
                self._respond(429, {
                    "error": "fixture_rate_limited",
                    "rateLimitRemaining": remaining,
                })
                return
            try:
                request = json.loads(self._read_request_body() or b"{}")
            except (ValueError, json.JSONDecodeError):
                self._respond(400, {"error": "invalid_json"})
                return

            request_type = str(request.get("type", "")).strip()
            print(
                json.dumps(
                    {"requestType": request_type, "user": request.get("user")},
                    separators=(",", ":"),
                ),
                flush=True,
            )
            if request_type == "meta":
                body = {"universe": [{"name": coin} for coin in universe],
                        "tokens": []}
            elif request_type.lower() == "clearinghousestate":
                body = {
                    "marginSummary": {
                        "accountValue": "1000",
                        "totalNtlPos": "0",
                        "totalRawUsd": "1000",
                        "totalMarginUsed": "0",
                    },
                    "assetPositions": [],
                    "time": args.source_timestamp,
                }
            elif request_type.lower() in ("userfills", "userfillsbytime"):
                body = fills_for(str(request.get("user", "")), request)
            else:
                body = {}
            self._respond(200, body)

        def _read_request_body(self) -> bytes:
            if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                length = int(self.headers.get("Content-Length", "0"))
                return self.rfile.read(length)
            chunks = bytearray()
            while True:
                size_line = self.rfile.readline().strip().split(b";", 1)[0]
                size = int(size_line, 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.extend(self.rfile.read(size))
                self.rfile.read(2)
            return bytes(chunks)

        def _respond(self, status: int, body: object) -> None:
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, fmt: str, *values: object) -> None:
            print(fmt % values, flush=True)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(
        json.dumps(
            {
                "fixture": "hyperliquid-info",
                "listen": f"127.0.0.1:{args.port}",
                "wallet": wallet,
                "tid": args.tid,
                "fills": len(configured_fills),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
