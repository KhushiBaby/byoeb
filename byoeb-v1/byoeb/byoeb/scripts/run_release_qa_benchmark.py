"""
Release QA benchmark: run curated positive/negative questions against the bot
and report metrics for qualitative release comparison.

This script is for manual benchmarking only. Do NOT use it in GitHub Actions or
any CI pipeline to fail the build. It always exits 0.

Usage:
  From byoeb-v1/byoeb:
    poetry run python byoeb/scripts/run_release_qa_benchmark.py
    poetry run python byoeb/scripts/run_release_qa_benchmark.py --base-url http://127.0.0.1:8000
    poetry run python byoeb/scripts/run_release_qa_benchmark.py --output benchmark_results.json

Requires chat app running on base-url.
"""
import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from fastmcp import Client
from fastmcp.client.client import CallToolResult
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Literal

import requests

from pydantic import BaseModel, TypeAdapter, ValidationError, field_validator

# Paths: script lives in tests/integration
_CURRENT_DIR = Path(__file__).resolve().parent
_BYOEB_ROOT = _CURRENT_DIR.parent.parent
if str(_BYOEB_ROOT) not in sys.path:
    sys.path.insert(0, str(_BYOEB_ROOT))

# Load keys.env so we see same env as chat app (e.g. MONGO_DB_CONNECTION_STRING)
_KEYS_ENV = _BYOEB_ROOT / "keys.env"
if _KEYS_ENV.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_KEYS_ENV, override=True)
    except ImportError:
        pass

DEFAULT_SET_PATH = _CURRENT_DIR / "release_qa_regression_set.json"
# Aligned with run_immunization_questions.py and test_already_onboarded.py:
# - Base URL: RECIEVE_URL or http://127.0.0.1:8000 (immunization uses 127.0.0.1; test_already_onboarded uses localhost:8000/receive)
DEFAULT_BASE_URL = "http://127.0.0.1:8000"

RESPONSE_SNIPPET_LEN = 300  # chars to show when printing responses


class BenchmarkItem(BaseModel):
    question: str
    expected: Literal["answered", "idk"]
    category: str = ""

    @field_validator("question")
    @classmethod
    def strip_question(cls, v: str) -> str:
        return v.strip()


def load_benchmark_set(path: Path) -> List[Dict[str, Any]]:
    """Load benchmark set from JSON and validate using pydantic."""
    try:
        items = TypeAdapter(list[BenchmarkItem]).validate_json(path.read_text(encoding="utf-8"))
    except ValidationError as e:
        raise ValueError(f"Benchmark set is invalid: {e}") from e
    return [item.model_dump() for item in items]


def snippet(text: str, max_len: int = RESPONSE_SNIPPET_LEN) -> str:
    """Return truncated text for display; normalize newlines."""
    if not text:
        return "(no response)"
    one_line = " ".join(text.split())
    return one_line[:max_len] + ("..." if len(one_line) > max_len else "")


def mask_mongo_connection_string(conn_str: Optional[str]) -> str:
    """Mask password in MongoDB connection string for safe console display."""
    if not conn_str or not conn_str.strip():
        return "(not set)"
    # mongodb://user:password@host or mongodb+srv://user:password@host
    masked = re.sub(r"(mongodb\+?srv?://[^:]+:)([^@]+)(@)", r"\1****\3", conn_str)
    if masked != conn_str:
        return masked
    # No password part matched; show scheme + host only (e.g. mongodb://host:27017)
    if len(conn_str) <= 60:
        return conn_str
    return conn_str[:60] + "..."


async def query(base_url: str, token: str, questions: list[str], timeout: int = 60, concurrency: int = 3) -> AsyncIterator[tuple[int, dict[str, Any]]]:
    sem = asyncio.Semaphore(concurrency)
    async def run_query(client: Client, query: str, index: int):
        async with sem:
            return index, await client.call_tool("asha_chat", {"message": query}, timeout=timeout, raise_on_error=False)

    def transform(question: str, response: CallToolResult):
        if response.is_error:
            return {
                "question": question,
                "response_text": None,
                "is_idk": False,
                "is_disambiguation": False,
                "message_category": None,
                "status": "mcp_error",
                "error": response.content,
            }

        is_disambiguation = response.data.category in ("text_disambiguation", "audio_disambiguation")
        is_idk = response.data.category in ("text_idk", "audio_idk", "audio_idk_reconfirmation")
        if not response.data.text:
            status = "no_response"
        elif is_disambiguation:
            status = "disambiguation"
        elif is_idk:
            status = "idk"
        else:
            status = "answered"
        return {
            "question": question,
            "response_text": response.data.text,
            "is_idk": is_idk,
            "is_disambiguation": is_disambiguation,
            "message_category": response.data.category,
            "status": status,
            "error": None,
        }

    async with Client(base_url.rstrip("/") + "/mcp", auth=token) as client:
        await client.call_tool("asha_register_user", {"data": {"name": "Release QA User", "language": "en", "state": "Karnataka"}}, raise_on_error=False)
        tasks = [run_query(client, q, i) for i, q in enumerate(questions)]
        for coro in asyncio.as_completed(tasks):
            index, response = await coro
            assert isinstance(response, CallToolResult)
            yield index, transform(questions[index], response)


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run release QA benchmark: curated positive/negative questions, report metrics (always exit 0)."
    )
    parser.add_argument(
        "--set-path",
        type=Path,
        default=DEFAULT_SET_PATH,
        help=f"Path to benchmark JSON (default: {DEFAULT_SET_PATH})",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base URL (e.g. http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--tenant-id",
        required=True,
        help="Tenant ID of an authenticated user",
    )
    parser.add_argument(
        "--username",
        required=True,
        help="Username of an authenticated user",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Password of an authenticated user",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Write results to JSON (optional; can include timestamp in filename)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrently running queries (default: 3)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of questions to run (default: all)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print final summary, not per-question progress",
    )
    parser.add_argument(
        "--show-responses",
        action="store_true",
        help="Print a snippet of the bot response after each question (to verify evaluation)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="Timeout in seconds for each MCP call (default: 90)",
    )
    # --mcp-timeout: CI-friendly alias for --timeout.
    parser.add_argument(
        "--mcp-timeout",
        type=int,
        default=None,
        dest="mcp_timeout",
        help="Timeout in seconds for each MCP call; overrides --timeout when provided",
    )
    args = parser.parse_args()

    # Let --mcp-timeout override --timeout if explicitly supplied
    if args.mcp_timeout is not None:
        args.timeout = args.mcp_timeout

    base_url = args.base_url
    if not base_url.startswith("http"):
        base_url = "http://" + base_url

    if not args.set_path.is_file():
        print(f"Benchmark set not found: {args.set_path}", file=sys.stderr)
        return 0  # still exit 0 per plan

    try:
        items = load_benchmark_set(args.set_path)
    except Exception as e:
        print(f"Failed to load benchmark set: {e}", file=sys.stderr)
        return 0

    if args.limit:
        items = items[: args.limit]

    positive_items = [x for x in items if x["expected"] == "answered"]
    negative_items = [x for x in items if x["expected"] == "idk"]

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    mode = "MCP (asha_chat)"
    if not args.quiet:
        print(f"Release QA Benchmark — {date_str}")
        print(f"Mode:        {mode}")
        print(f"Loaded {len(items)} items ({len(positive_items)} positive, {len(negative_items)} negative)")
        print(f"Base URL:    {base_url}")
        db_conn = os.getenv("MONGO_DB_CONNECTION_STRING")
        print(f"Database (MONGO_DB_CONNECTION_STRING): {mask_mongo_connection_string(db_conn)}")
        print()

    headers = {"Content-Type": "application/x-www-form-urlencoded", "X-Tenant-ID": str(args.tenant_id)}
    response = requests.post(f"{base_url}/auth/token/issue", headers=headers, data={"username": args.username, "password": args.password})
    response.raise_for_status()
    token = response.cookies.get("asha_auth_token")

    results: List[Dict[str, Any]] = []
    async for i, res in query(base_url, token, [i["question"] for i in items], timeout=args.timeout, concurrency=args.concurrency):
        item = items[i]
        q = item["question"]
        expected = item["expected"]
        res["expected"] = expected
        res["category"] = item.get("category", "")
        res["passed"] = (
            (expected == "answered" and res["status"] == "answered")
            or (expected == "idk" and res["status"] in ("idk", "disambiguation"))
        )
        results.append(res)
        if not args.quiet:
            print(f"[{len(results)}/{len(items)}] {q[:70]}{'...' if len(q) > 70 else ''}")
        if not args.quiet:
            actual = res["status"]
            mark = "✓" if res["passed"] else "✗"
            print(f"  -> {mark} expected={expected}, actual={actual}")
            if args.show_responses or True:
                print(f"     Bot: {snippet(res.get('response_text') or '')}")

    # Benchmark metrics
    pos_results = [r for r in results if r["expected"] == "answered"]
    neg_results = [r for r in results if r["expected"] == "idk"]
    pos_passed = sum(1 for r in pos_results if r["passed"])
    neg_passed = sum(1 for r in neg_results if r["passed"])
    pos_total = len(pos_results)
    neg_total = len(neg_results)
    pos_pct = (100.0 * pos_passed / pos_total) if pos_total else 0.0
    neg_pct = (100.0 * neg_passed / neg_total) if neg_total else 0.0
    disambiguation_count = sum(1 for r in results if r.get("status") == "disambiguation")

    # Summary
    print()
    print("---------------------------------")
    print(f"Positive (expected answered):  {pos_passed}/{pos_total} ({pos_pct:.1f}%)")
    for r in pos_results:
        if not r["passed"]:
            reason = r["status"]
            q_display = r["question"][:60] + "..." if len(r["question"]) > 60 else r["question"]
            print(f"  Mismatch: \"{q_display}\" -> got {reason}")
            print(f"    Bot: {snippet(r.get('response_text') or '')}")
    print(f"Negative (expected IDK):       {neg_passed}/{neg_total} ({neg_pct:.1f}%)")
    for r in neg_results:
        if not r["passed"]:
            q_display = r["question"][:60] + "..." if len(r["question"]) > 60 else r["question"]
            print(f"  Answered: \"{q_display}\"")
            print(f"    Bot: {snippet(r.get('response_text') or '')}")
    if disambiguation_count:
        print(f"Disambiguation (not full answer): {disambiguation_count} question(s)")
    print("---------------------------------")
    print("Run again after the next release and compare metrics to gauge progress.")
    print("Do not use this script in CI to fail the build.")
    print()

    # Optional JSON output
    if args.output:
        out_path = args.output
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "base_url": base_url,
            "mode": "mcp",
            "summary": {
                "total": len(results),
                "positive_total": pos_total,
                "positive_passed": pos_passed,
                "positive_pct": round(pos_pct, 2),
                "negative_total": neg_total,
                "negative_passed": neg_passed,
                "negative_pct": round(neg_pct, 2),
                "disambiguation_count": disambiguation_count,
            },
            "results": [
                {
                    "question": r["question"],
                    "expected": r["expected"],
                    "category": r.get("category", ""),
                    "actual_status": r["status"],
                    "message_category": r.get("message_category"),
                    "is_disambiguation": r.get("is_disambiguation", False),
                    "passed": r["passed"],
                    "response_text": (r.get("response_text") or "")[:2000],
                }
                for r in results
            ],
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        if not args.quiet:
            print(f"Results written to {out_path}")

    # Always exit 0 (benchmark only; do not fail CI)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
