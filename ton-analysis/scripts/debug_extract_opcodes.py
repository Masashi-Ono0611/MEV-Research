#!/usr/bin/env python3
"""
One-shot extractor for STON.fi router transactions focusing on opcodes:
- in_msg op_code == 0x7362d09c (Jetton Notify)
- out_msgs op_code == 0x0f8a7ea5 (Jetton Transfer)

Fetches exactly one page (limit=30) with no retries/pagination, groups by query_id,
and writes paired Notify+SwapV2 / PayToV2+Transfer bundles to NDJSON under ton-analysis/data.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation, getcontext

import requests

# Constants
LIMIT = 30
ROUTER = os.getenv("TON_ROUTER", "EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3")
BASE_URL = (os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain"
API_KEY = os.getenv("NEXT_PUBLIC_TON_API_KEY") or os.getenv("TON_API_KEY_MAINNET")
OUT_PATH = os.path.join(os.path.dirname(__file__), "../data/opcode_debug.ndjson")

# for rate calculation
getcontext().prec = 28

IN_OP_NOTIFY = "0x7362d09c"  # Jetton Notify
IN_OP_PAY_V2 = "0x657b54f5"  # Stonfi Pay To V2
OUT_OP_SWAP_V2 = "0x6664de2a"  # Stonfi Swap V2
OUT_OP_TRANSFER = "0x0f8a7ea5"  # Jetton Transfer

# Token wallets (direction判定用)
PTON_WALLET = "0:922d627d7d8edbd00e4e23bdb0c54a76ee5e1f46573a1af4417857fa3e23e91f"
USDT_WALLET = "0:9220c181a6cfeacd11b7b8f62138df1bb9cc82b6ed2661d2f5faee204b3efb20"


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

    # Group by query_id and role
    buckets: Dict[str, Dict[str, Any]] = {}
    for tx in txs:
        in_msg = tx.get("in_msg") or {}
        out_msgs = tx.get("out_msgs") or []

        in_op = (in_msg.get("op_code", "") or "").lower()
        out_ops = [(om.get("op_code", "") or "").lower() for om in out_msgs]

        # Determine role and key query_id
        role = None
        qid = None
        if in_op in {IN_OP_NOTIFY, IN_OP_PAY_V2}:
            qid = str((in_msg.get("decoded_body") or {}).get("query_id", ""))
            role = "notify" if in_op == IN_OP_NOTIFY else "pay"
        # prefer decoded_body.query_id from matching out when in_op absent
        if qid in (None, ""):
            for om in out_msgs:
                od = om.get("decoded_body") or {}
                qid = str(od.get("query_id", ""))
                if qid:
                    break

        if not qid:
            continue

        bucket = buckets.setdefault(qid, {"notify": None, "swap": None, "pay": None, "transfer": None})

        if role == "notify":
            bucket["notify"] = {"tx_hash": tx.get("hash"), "in_msg": in_msg}
            # capture swap if present
            for om in out_msgs:
                if (om.get("op_code", "") or "").lower() == OUT_OP_SWAP_V2:
                    bucket["swap"] = {"tx_hash": tx.get("hash"), "out_msg": om}
        elif role == "pay":
            bucket["pay"] = {"tx_hash": tx.get("hash"), "in_msg": in_msg}
            for om in out_msgs:
                if (om.get("op_code", "") or "").lower() == OUT_OP_TRANSFER:
                    bucket["transfer"] = {"tx_hash": tx.get("hash"), "out_msg": om}
        else:
            # purely out_msg hits (rare) could be swap/transfer; attach if not set
            for om in out_msgs:
                op = (om.get("op_code", "") or "").lower()
                if op == OUT_OP_SWAP_V2 and bucket.get("swap") is None:
                    bucket["swap"] = {"tx_hash": tx.get("hash"), "out_msg": om}
                if op == OUT_OP_TRANSFER and bucket.get("transfer") is None:
                    bucket["transfer"] = {"tx_hash": tx.get("hash"), "out_msg": om}

    # Emit aggregated rows
    rows = []
    for qid, parts in buckets.items():
        if not any(parts.values()):
            continue
        direction = infer_direction(parts)
        meta = extract_meta(parts)
        amounts = compute_amounts(parts, direction)
        rows.append({"query_id": qid, "direction": direction, **meta, **amounts, **parts})

    os.makedirs(os.path.dirname(os.path.abspath(OUT_PATH)), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"extracted {len(rows)} query_id bundles -> {os.path.abspath(OUT_PATH)}")
    return 0


def infer_direction(parts: Dict[str, Any]) -> str:
    """Infer swap direction using notify sender or transfer destination.

    Priority:
    - transfer.out_msg.decoded_body.destination (jetton transfer recipient)
    - notify.in_msg.decoded_body.sender (jetton notify sender wallet)
    - swap.out_msg.decoded_body.token_wallet1 as the opposite side indicator
    """

    def norm(addr: str) -> str:
        return (addr or "").lower()

    # 1) transfer destination
    transfer = parts.get("transfer") or {}
    t_dest = norm(((transfer.get("out_msg") or {}).get("decoded_body") or {}).get("destination", ""))
    if t_dest == norm(USDT_WALLET):
        return "TON->USDT"
    if t_dest == norm(PTON_WALLET):
        return "USDT->TON"

    # 2) notify sender
    notify = parts.get("notify") or {}
    n_sender = norm(((notify.get("in_msg") or {}).get("decoded_body") or {}).get("sender", ""))
    if n_sender == norm(USDT_WALLET):
        return "USDT->TON"
    if n_sender == norm(PTON_WALLET):
        return "TON->USDT"

    # 3) swap token_wallet1 hint (token1 is usually counter-side)
    swap = parts.get("swap") or {}
    token_wallet1 = norm(((swap.get("out_msg") or {}).get("decoded_body") or {}).get("dex_payload", {}).get("token_wallet1", ""))
    if token_wallet1 == norm(USDT_WALLET):
        return "TON->USDT"
    if token_wallet1 == norm(PTON_WALLET):
        return "USDT->TON"

    return "unknown"


def extract_meta(parts: Dict[str, Any]) -> Dict[str, Any]:
    """Collect representative lt/utime from notify/transfer when available."""

    notify = parts.get("notify") or {}
    transfer = parts.get("transfer") or {}

    def pick_lt_utime(msg: Dict[str, Any]) -> Dict[str, Any]:
        lt = (msg.get("in_msg") or msg.get("out_msg") or {}).get("created_lt")
        utime = (msg.get("in_msg") or msg.get("out_msg") or {}).get("created_at")
        return {"lt": lt, "utime": utime}

    n_meta = pick_lt_utime(notify) if notify else {"lt": None, "utime": None}
    t_meta = pick_lt_utime(transfer) if transfer else {"lt": None, "utime": None}

    # prefer notify, fallback to transfer
    lt = n_meta.get("lt") or t_meta.get("lt")
    utime = n_meta.get("utime") or t_meta.get("utime")

    return {"lt": lt, "utime": utime}


def compute_amounts(parts: Dict[str, Any], direction: str) -> Dict[str, Any]:
    """Compute in_amount, out_amount, and rate (out/in) using Jetton amounts.

    TON->USDT: in = notify.amount (pTON), out = transfer.amount (USDT)
    USDT->TON: in = swap.right_amount (USDT), out = transfer.amount (pTON)
    Falls back to pay.additional_info when needed. Rate is Decimal(out)/Decimal(in) if valid.
    """

    def d(val: Any) -> Optional[Decimal]:
        try:
            return Decimal(str(val))
        except (InvalidOperation, TypeError):
            return None

    notify = (parts.get("notify") or {}).get("in_msg") or {}
    swap = (parts.get("swap") or {}).get("out_msg") or {}
    pay = (parts.get("pay") or {}).get("in_msg") or {}
    transfer = (parts.get("transfer") or {}).get("out_msg") or {}

    in_amt = None
    out_amt = None

    if direction == "TON->USDT":
        # in from notify.amount or pay.amount0_out
        in_amt = d(((notify.get("decoded_body") or {}).get("amount")))
        if in_amt is None:
            in_amt = d(((pay.get("decoded_body") or {}).get("amount0_out")))
        # out from transfer.amount
        out_amt = d(((transfer.get("decoded_body") or {}).get("amount")))
    elif direction == "USDT->TON":
        # in from swap.right_amount (USDT) or pay.amount1_out
        in_amt = d(((swap.get("decoded_body") or {}).get("right_amount")))
        if in_amt is None:
            in_amt = d(((pay.get("decoded_body") or {}).get("amount1_out")))
        # out from transfer.amount (pTON)
        out_amt = d(((transfer.get("decoded_body") or {}).get("amount")))

    rate = None
    if in_amt and out_amt and in_amt != 0:
        try:
            rate = (out_amt / in_amt).quantize(Decimal("1.000000000000000000"))
        except InvalidOperation:
            rate = None

    return {
        "in_amount": str(in_amt) if in_amt is not None else None,
        "out_amount": str(out_amt) if out_amt is not None else None,
        "rate": str(rate) if rate is not None else None,
    }


if __name__ == "__main__":
    sys.exit(main())
