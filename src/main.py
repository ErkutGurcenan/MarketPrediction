#!/usr/bin/env python3
import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import csv
import os


import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

GAMMA_BASE = "https://gamma-api.polymarket.com"
GAMMA_MARKETS = f"{GAMMA_BASE}/markets"
GAMMA_EVENT_BY_SLUG = f"{GAMMA_BASE}/events/slug"   # /{slug}
CLOB_MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def time_ns() -> int:
    return time.time_ns()

def ns_to_ms(ns: int) -> float:
    return ns / 1_000_000.0

def http_get_json(url: str, timeout_s: int = 20):
    req = Request(url, headers={"User-Agent": "pm-tennis-step5/1.1"})
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def best_from_levels(levels, side: str):
    if not isinstance(levels, list) or not levels:
        return None
    parsed = []
    for lvl in levels:
        if not isinstance(lvl, dict):
            continue
        p = safe_float(lvl.get("price"))
        s = safe_float(lvl.get("size"))
        if p is None or s is None:
            continue
        parsed.append((p, s))
    if not parsed:
        return None
    return max(parsed, key=lambda t: t[0]) if side == "bid" else min(parsed, key=lambda t: t[0])

def midpoint(bb, ba):
    if bb is None or ba is None:
        return None
    return (bb + ba) / 2.0

def extract_token_ids_any(obj: dict):
    """
    Robust extraction:
    - accepts list[str], list[int], tuple, set
    - accepts a single string
    - filters out empty/None
    """
    if not isinstance(obj, dict):
        return None, None

    v = obj.get("clobTokenIds")
    if v is not None:
        # case 1: list/tuple/set of ids
        if isinstance(v, (list, tuple, set)):
            ids = [str(x) for x in v if x is not None and str(x).strip() != ""]
            if ids:
                return ids, obj.get("outcomes")
        # case 2: single id string
        if isinstance(v, str) and v.strip():
            return [v.strip()], obj.get("outcomes")

    # fallback singular
    v2 = obj.get("clobTokenId")
    if isinstance(v2, str) and v2.strip():
        return [v2.strip()], obj.get("outcomes")

    return None, None


def fetch_market_by_slug(slug: str):
    # docs show slug is a string[] filter; use slug[]= style to be safe
    qs = urlencode([("slug", slug)])
    url = f"{GAMMA_MARKETS}?{qs}"
    data = http_get_json(url)
    if isinstance(data, list) and data:
        return data[0]
    raise RuntimeError(f"No market found for slug={slug}")

def fetch_event_by_slug(slug: str):
    url = f"{GAMMA_EVENT_BY_SLUG}/{slug}"
    return http_get_json(url)

def pick_market_from_event(event_obj: dict):
    # Many event payloads include markets list
    mkts = event_obj.get("markets")
    if isinstance(mkts, list) and mkts:
        for m in mkts:
            if isinstance(m, dict):
                return m
    return None

async def ws_subscribe_and_print(asset_ids, slug: str, question: str, out_dir: str, print_every: int = 1):
    print(f"[{utc_now_iso()}] Connecting WS: {CLOB_MARKET_WSS}", flush=True)
    print(f"[{utc_now_iso()}] Subscribing assets_ids count={len(asset_ids)}", flush=True)

    writer = CsvWriter(out_dir, "polymarket_tennis.csv")

    reconnect_delay = 1.0
    msg_count = 0

    try:
        while True:
            try:
                async with websockets.connect(
                    CLOB_MARKET_WSS, ping_interval=20, ping_timeout=20, close_timeout=5
                ) as ws:
                    reconnect_delay = 1.0
                    sub = {"type": "MARKET", "assets_ids": asset_ids}
                    await ws.send(json.dumps(sub))
                    print(f"[{utc_now_iso()}] ✅ subscribed, waiting for messages...", flush=True)

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            print(f"[{utc_now_iso()}] (heartbeat) connected, waiting for updates…", flush=True)
                            continue

                        recv_ns = time_ns()

                        try:
                            parsed = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        # Normalize: sometimes dict, sometimes list[dict]
                        if isinstance(parsed, dict):
                            msgs = [parsed]
                        elif isinstance(parsed, list):
                            msgs = [m for m in parsed if isinstance(m, dict)]
                        else:
                            continue

                        for msg in msgs:
                            event_type = str(msg.get("event_type") or msg.get("type") or "unknown")
                            exch_ts_raw = msg.get("timestamp")
                            exch_ts_ms = safe_int(exch_ts_raw)  # best-effort
                            asset_id = msg.get("asset_id")

                            bb = ba = mid = None
                            if event_type == "book":
                                bid = best_from_levels(msg.get("bids"), "bid")
                                ask = best_from_levels(msg.get("asks"), "ask")
                                if bid:
                                    bb = bid[0]
                                if ask:
                                    ba = ask[0]
                                mid = midpoint(bb, ba)

                            proc_end_ns = time_ns()

                            # Latencies (best-effort if exchange ts exists)
                            net_ms = None
                            e2e_ms = None
                            if exch_ts_ms is not None:
                                net_ms = ns_to_ms(recv_ns) - float(exch_ts_ms)
                                e2e_ms = ns_to_ms(proc_end_ns) - float(exch_ts_ms)
                            proc_ms = ns_to_ms(proc_end_ns - recv_ns)

                            # ✅ Write to CSV (you can choose to log only book events if you want)
                            row = {
                                "utc_iso": utc_now_iso(),
                                "slug": slug,
                                "question": question,
                                "asset_id": asset_id,
                                "event_type": event_type,
                                "best_bid": bb,
                                "best_ask": ba,
                                "mid": mid,
                                "exch_ts_raw": exch_ts_raw,
                                "recv_ts_ns": recv_ns,
                                "proc_end_ts_ns": proc_end_ns,
                                "proc_latency_ms": proc_ms,
                                "net_latency_ms": net_ms,
                                "e2e_latency_ms": e2e_ms,
                                "raw_json": raw if len(raw) < 20000 else raw[:20000],
                            }
                            if event_type == "book":
                                writer.write(row)

                            msg_count += 1
                            if msg_count % print_every == 0:
                                print(
                                    f"[{utc_now_iso()}] n={msg_count} type={event_type} asset={asset_id} "
                                    f"bb={bb} ba={ba} mid={mid} net_ms={net_ms} proc_ms={proc_ms:.3f}",
                                    flush=True
                                )

            except (ConnectionClosedError, ConnectionClosedOK, OSError) as e:
                print(
                    f"[{utc_now_iso()}] WS disconnected: {e}. Reconnecting in {reconnect_delay:.1f}s...",
                    flush=True
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30.0)

    finally:
        writer.close()

            
            
            
def normalize_token_ids(token_ids):
    import json

    if token_ids is None:
        return []

    # Case: already a string
    if isinstance(token_ids, str):
        s = token_ids.strip()
        if not s:
            return []
        # If it looks like a JSON list string: '["id1","id2"]'
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x).strip() for x in parsed if x is not None and str(x).strip()]
            except Exception:
                pass
        return [s]

    # Case: list/tuple/set
    if isinstance(token_ids, (list, tuple, set)):
        token_ids = list(token_ids)

        # If single element and it is a list -> flatten
        if len(token_ids) == 1 and isinstance(token_ids[0], list):
            token_ids = token_ids[0]

        out = []
        for x in token_ids:
            if x is None:
                continue
            # If element itself is a JSON list string, expand it
            if isinstance(x, str):
                s = x.strip()
                if s.startswith("[") and s.endswith("]"):
                    try:
                        parsed = json.loads(s)
                        if isinstance(parsed, list):
                            out.extend([str(y).strip() for y in parsed if y is not None and str(y).strip()])
                            continue
                    except Exception:
                        pass
                if s:
                    out.append(s)
            else:
                s = str(x).strip()
                if s:
                    out.append(s)

        # de-dup while keeping order
        seen = set()
        deduped = []
        for s in out:
            if s not in seen:
                deduped.append(s)
                seen.add(s)
        return deduped

    return []





CSV_COLUMNS = [
    "utc_iso",
    "slug",
    "question",
    "asset_id",
    "event_type",
    "best_bid",
    "best_ask",
    "mid",
    "exch_ts_raw",
    "recv_ts_ns",
    "proc_end_ts_ns",
    "proc_latency_ms",
    "net_latency_ms",
    "e2e_latency_ms",
    "raw_json",
]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

class CsvWriter:
    def __init__(self, out_dir: str, filename: str):
        ensure_dir(out_dir)
        self.path = os.path.join(out_dir, filename)
        self._fh = open(self.path, "a", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._fh, fieldnames=CSV_COLUMNS)
        if self._fh.tell() == 0:
            self._w.writeheader()
            self._fh.flush()

    def write(self, row: dict):
        self._w.writerow(row)
        self._fh.flush()

    def close(self):
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass





def main():
    p = argparse.ArgumentParser()
    p.add_argument("--slug", required=True, help="Slug from search results, e.g. wta-uchijim-bondar-2026-02-13")
    p.add_argument("--print-every", type=int, default=1)
    p.add_argument("--out", default="data", help="Output directory (default: data)")
    p.add_argument("--debug-keys", action="store_true", help="Print keys if token ids missing")
    args = p.parse_args()

    slug = args.slug
    print(f"[{utc_now_iso()}] Trying /markets?slug=... for slug={slug}", flush=True)

    token_ids = None
    outcomes = None
    question = ""  # ✅ will be filled from whichever path succeeds

    # 1) Try market by slug
    try:
        market = fetch_market_by_slug(slug)
        question = str(market.get("question") or "")
        print(f"[{utc_now_iso()}] market question: {question}", flush=True)
        print(f"[{utc_now_iso()}] market enableOrderBook: {market.get('enableOrderBook')}", flush=True)

        token_ids, outcomes = extract_token_ids_any(market)
        token_ids = normalize_token_ids(token_ids)

        if token_ids:
            print(f"[{utc_now_iso()}] ✅ token ids found via /markets", flush=True)

    except Exception as e:
        print(f"[{utc_now_iso()}] /markets lookup failed: {e}", flush=True)

    # 2) If not found, treat slug as event slug
    if not token_ids:
        print(f"[{utc_now_iso()}] Trying /events/slug/{{slug}} ...", flush=True)
        event_obj = fetch_event_by_slug(slug)

        print(f"[{utc_now_iso()}] event title: {event_obj.get('title')}", flush=True)
        print(f"[{utc_now_iso()}] event enableOrderBook: {event_obj.get('enableOrderBook')}", flush=True)

        m = pick_market_from_event(event_obj)
        if m:
            question = str(m.get("question") or "")
            print(f"[{utc_now_iso()}] event->market question: {question}", flush=True)

            token_ids, outcomes = extract_token_ids_any(m)
            token_ids = normalize_token_ids(token_ids)

        if (not token_ids) and args.debug_keys:
            print("DEBUG event keys:", list(event_obj.keys()), flush=True)
            if m:
                print("DEBUG market keys:", list(m.keys()), flush=True)
                print("DEBUG clobTokenIds type:", type(m.get("clobTokenIds")), flush=True)
                print("DEBUG clobTokenIds value repr:", repr(m.get("clobTokenIds"))[:300], flush=True)
                print("DEBUG outcomes type:", type(m.get("outcomes")), flush=True)
                print("DEBUG outcomes repr:", repr(m.get("outcomes"))[:300], flush=True)

    if not token_ids:
        raise RuntimeError(
            "Could not find CLOB token IDs via /markets or /events/slug. "
            "Use --debug-keys to inspect payload keys."
        )

    print(f"[{utc_now_iso()}] clob token count={len(token_ids)}", flush=True)
    if isinstance(outcomes, list) and outcomes:
        for i, tid in enumerate(token_ids):
            label = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
            print(f"  - {label}: {tid}", flush=True)
    else:
        for tid in token_ids:
            print(f"  - {tid}", flush=True)

    # ✅ IMPORTANT: call with new signature
    asyncio.run(
        ws_subscribe_and_print(
            token_ids,
            slug=slug,
            question=question,
            out_dir=args.out,
            print_every=args.print_every,
        )
    )



if __name__ == "__main__":
    main()
