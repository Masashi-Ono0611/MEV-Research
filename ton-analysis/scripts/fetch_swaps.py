"""
Swap log fetcher (skeleton) for TON DEX (e.g., STON.fi USDT<>TON pair).
- Keeps dependencies minimal (requests only) and accepts endpoint/pair via env or CLI.
- Pagination and time-range filtering placeholders; adapt to actual API (REST/subgraph/RPC).
- Designed for 24h snapshot pulls to a newline-delimited JSON file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

DEFAULT_OUT = "swaps_24h.ndjson"


def parse_ts(ts: str) -> int:
    """Parse ISO8601 or Unix seconds to int seconds."""
    try:
        return int(ts)
    except ValueError:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())


def utc_now_s() -> int:
    return int(datetime.now(timezone.utc).timestamp())


@dataclass
class SwapLog:
    tx_hash: str
    block_height: int
    timestamp: int
    sender: str
    amount_in: str
    amount_out: str
    raw: Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(
            {
                "tx_hash": self.tx_hash,
                "block_height": self.block_height,
                "timestamp": self.timestamp,
                "sender": self.sender,
                "amount_in": self.amount_in,
                "amount_out": self.amount_out,
                "raw": self.raw,
            },
            ensure_ascii=False,
        )


def fetch_page(
    *,
    api_url: str,
    pair_id: str,
    start_ts: int,
    end_ts: int,
    cursor: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    """Placeholder: adapt to the actual DEX API/Subgraph.

    Expected response shape (example):
    {
        "data": [{...swap...}],
        "next_cursor": "..." or None
    }
    """
    # TODO: Replace with real query parameters / GraphQL payload.
    params = {
        "pair": pair_id,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit": limit,
    }
    if cursor:
        params["cursor"] = cursor

    resp = requests.get(api_url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_swaps(payload: Dict[str, Any]) -> Iterable[SwapLog]:
    # TODO: Map real response fields to SwapLog. This is an example structure.
    for item in payload.get("data", []):
        yield SwapLog(
            tx_hash=item.get("tx_hash", ""),
            block_height=int(item.get("block_number", 0)),
            timestamp=int(item.get("timestamp", 0)),
            sender=item.get("sender", ""),
            amount_in=str(item.get("amount_in", "")),
            amount_out=str(item.get("amount_out", "")),
            raw=item,
        )


def fetch_all(
    *, api_url: str, pair_id: str, start_ts: int, end_ts: int, limit: int = 200
) -> List[SwapLog]:
    cursor: Optional[str] = None
    swaps: List[SwapLog] = []
    while True:
        payload = fetch_page(
            api_url=api_url,
            pair_id=pair_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor=cursor,
            limit=limit,
        )
        page_swaps = list(parse_swaps(payload))
        swaps.extend(page_swaps)
        cursor = payload.get("next_cursor")
        if not cursor or not page_swaps:
            break
    return swaps


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch swap logs for a TON DEX pair (24h snapshot).",
    )
    parser.add_argument("--api-url", default=os.getenv("TON_DEX_API"), help="REST/GraphQL endpoint")
    parser.add_argument("--pair-id", default=os.getenv("TON_DEX_PAIR"), help="Pair identifier (API specific)")
    parser.add_argument("--start", help="Start time (unix seconds or ISO8601). Defaults to now-24h.")
    parser.add_argument("--end", help="End time (unix seconds or ISO8601). Defaults to now.")
    parser.add_argument("--out", default=DEFAULT_OUT, help="NDJSON output path")
    parser.add_argument("--limit", type=int, default=200, help="Page size (API dependent)")
    args = parser.parse_args(argv)

    if not args.api_url or not args.pair_id:
        parser.error("--api-url and --pair-id are required (or set TON_DEX_API / TON_DEX_PAIR env)")

    end_ts = parse_ts(args.end) if args.end else utc_now_s()
    start_ts = parse_ts(args.start) if args.start else end_ts - 24 * 3600

    swaps = fetch_all(
        api_url=args.api_url,
        pair_id=args.pair_id,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=args.limit,
    )

    with open(args.out, "w", encoding="utf-8") as f:
        for s in swaps:
            f.write(s.to_json() + "\n")

    print(f"fetched {len(swaps)} swaps -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
