#!/usr/bin/env python3
"""Loopback-only Hyperliquid /info fixture for packaged HOT certification."""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--tid", type=int, required=True)
    parser.add_argument("--source-timestamp", type=int, required=True)
    parser.add_argument("--start-position", default="0")
    args = parser.parse_args()

    wallet = args.wallet.lower()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - HTTP handler contract
            if self.path == "/health":
                self._respond(200, {"status": "UP"})
            else:
                self._respond(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802 - HTTP handler contract
            if self.path != "/info":
                self._respond(404, {"error": "not_found"})
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
                body = {"universe": [{"name": "HYPE"}], "tokens": []}
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
                body = [
                    {
                        "coin": "HYPE",
                        "px": "21.25",
                        "sz": "1",
                        "side": "B",
                        "time": args.source_timestamp,
                        "startPosition": args.start_position,
                        "dir": "Open Long",
                        "closedPnl": "0",
                        "hash": f"0xpackaged{args.tid}",
                        "fee": "0.01",
                        "tid": args.tid,
                        "oid": args.tid,
                    }
                ] if str(request.get("user", "")).lower() == wallet else []
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
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
