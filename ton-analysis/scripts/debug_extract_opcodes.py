#!/usr/bin/env python3
"""
One-shot extractor for STON.fi router transactions focusing on opcodes:
- in_msg op_code == 0x7362d09c (Jetton Notify)
- out_msgs op_code == 0x0f8a7ea5 (Jetton Transfer)

Fetches exactly one page (limit=30) with no retries/pagination and writes
matching transactions to NDJSON under ton-analysis/data.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests

# Constants
LIMIT = 30
ROUTER = os.getenv("TON_ROUTER", "EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3")
BASE_URL = (os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain"
API_KEY = os.getenv("NEXT_PUBLIC_TON_API_KEY") or os.getenv("TON_API_KEY_MAINNET")
OUT_PATH = os.path.join(os.path.dirname(__file__), "../data/opcode_debug.ndjson")

IN_OP = "0x7362d09c"  # Jetton Notify
OUT_OP = "0x0f8a7ea5"  # Jetton Transfer


def fetch_once() -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/accounts/{ROUTER}/transactions"
    headers = {"Accept": "application/json"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    resp = requests.get(url, params={"limit": LIMIT}, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json().get("transactions", [])


def main(argv: Optional[List[str]] = None) -> int:
    try:
        txs = fetch_once()
    except Exception as exc:  # noqa: BLE001
        print(f"error: failed to fetch txs: {exc}", file=sys.stderr)
        return 1

    hits = []
    for tx in txs:
        in_msg = tx.get("in_msg") or {}
        out_msgs = tx.get("out_msgs") or []

        in_match = (in_msg.get("op_code", "").lower() == IN_OP)
        out_filtered = [om for om in out_msgs if (om.get("op_code", "").lower() == OUT_OP)]
        out_match = bool(out_filtered)

        if not (in_match or out_match):
            continue

        hits.append(
            {
                "tx_hash": tx.get("hash"),
                "lt": tx.get("lt"),
                "utime": tx.get("utime"),
                "in_op": in_msg.get("op_code"),
                "out_ops": [om.get("op_code") for om in out_filtered],
                "in_msg": in_msg,
                "out_msgs": out_filtered if out_filtered else out_msgs,
            }
        )

    os.makedirs(os.path.dirname(os.path.abspath(OUT_PATH)), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for row in hits:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"extracted {len(hits)} txs -> {os.path.abspath(OUT_PATH)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
