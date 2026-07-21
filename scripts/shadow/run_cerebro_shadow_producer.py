#!/usr/bin/env python3
"""Bounded, read-only RFC-014 producer. Input must already be normalized JSONL."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
from atlaspump.shadow_producer import JsonlProducer, to_stream_event  # noqa: E402


def main(args: argparse.Namespace) -> int:
    producer, started, tokens, written, failed = JsonlProducer(args.output_root, args.rotate_bytes), time.monotonic(), set(), 0, 0
    source = sys.stdin if args.input == "-" else Path(args.input).open(encoding="utf-8")
    try:
        for line in source:
            if time.monotonic() - started >= args.max_seconds or len(tokens) >= args.max_tokens:
                break
            try:
                raw = json.loads(line)
                event = to_stream_event(raw)
                if event["token_mint"] not in tokens and len(tokens) >= args.max_tokens:
                    break
                tokens.add(event["token_mint"])
                written += int(producer.append(event))
            except Exception as exc:
                failed += 1
                producer.dead_letter(line.rstrip(), exc)
    finally:
        if source is not sys.stdin:
            source.close()
    print(json.dumps({"written": written, "tokens": len(tokens), "errors": failed, "mode": "shadow_only"}))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--input", default="-")
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--rotate-bytes", type=int, default=128 * 1024 * 1024)
    raise SystemExit(main(parser.parse_args()))
