from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from utils.logging import get_logger


@dataclass(frozen=True)
class GistRef:
    gist_id: str
    html_url: str | None = None


def _read_tail_lines(path: str, lines: int) -> str:
    """
    Read approximately the last N lines from a file without loading it all.
    Best-effort (good enough for log tails).
    """
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    if lines <= 0:
        return ""

    # Read from the end in chunks until we have enough newlines.
    chunk = 8192
    data = b""
    with p.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        pos = size
        while pos > 0 and data.count(b"\n") <= lines:
            step = min(chunk, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step) + data

    try:
        txt = data.decode("utf-8", errors="replace")
    except Exception:
        txt = str(data)
    parts = txt.splitlines()[-lines:]
    return "\n".join(parts).strip()


def _build_report_md(store: Any, *, log_tail: str | None) -> str:
    pnl = store.fetch_latest_pnl() or {}
    positions = store.fetch_latest_positions(limit=200)
    orders = store.fetch_recent_orders(limit=50)
    fills = store.fetch_recent_fills(limit=50)

    open_pos = [p for p in positions if float(p.get("position") or 0.0) != 0.0]
    flat_pos = [p for p in positions if float(p.get("position") or 0.0) == 0.0 and float(p.get("realized_pnl") or 0.0) != 0.0]

    def _fmt_usd(x: Any) -> str:
        try:
            v = float(x)
        except Exception:
            v = 0.0
        return f"${v:,.2f}"

    now = time.time()
    out: list[str] = []
    out.append("# Trading snapshot")
    out.append("")
    out.append(f"- generated: `{now:.0f}`")
    out.append("")
    out.append("## PnL")
    out.append("")
    out.append(f"- total: **{_fmt_usd(pnl.get('total_pnl', 0.0))}**")
    out.append(f"- unrealized: {_fmt_usd(pnl.get('total_unrealized', 0.0))}")
    out.append(f"- realized: {_fmt_usd(pnl.get('total_realized', 0.0))}")
    out.append("")

    out.append(f"## Open positions ({len(open_pos)})")
    out.append("")
    out.append("| market_id | pos | avg | mark | uPnL | rPnL |")
    out.append("|---|---:|---:|---:|---:|---:|")
    for p in sorted(open_pos, key=lambda r: abs(float(r.get("unrealized_pnl") or 0.0)), reverse=True)[:20]:
        out.append(
            f"| `{p.get('market_id')}` | {float(p.get('position') or 0.0):.2f} | {float(p.get('avg_price') or 0.0):.3f} | "
            f"{float(p.get('mark_price') or 0.0):.3f} | {_fmt_usd(p.get('unrealized_pnl') or 0.0)} | {_fmt_usd(p.get('realized_pnl') or 0.0)} |"
        )
    out.append("")

    out.append(f"## Recently closed (flat, realized != 0) ({len(flat_pos)})")
    out.append("")
    out.append("| market_id | rPnL | last mark |")
    out.append("|---|---:|---:|")
    for p in sorted(flat_pos, key=lambda r: abs(float(r.get("realized_pnl") or 0.0)), reverse=True)[:20]:
        out.append(
            f"| `{p.get('market_id')}` | {_fmt_usd(p.get('realized_pnl') or 0.0)} | {float(p.get('mark_price') or 0.0):.3f} |"
        )
    out.append("")

    out.append("## Recent fills (50)")
    out.append("")
    out.append("| ts | market_id | side | px | size |")
    out.append("|---:|---|---|---:|---:|")
    for f in fills[:50]:
        out.append(
            f"| {float(f.get('ts') or 0.0):.0f} | `{f.get('market_id')}` | {f.get('side')} | {float(f.get('price') or 0.0):.3f} | {float(f.get('size') or 0.0):.2f} |"
        )
    out.append("")

    out.append("## Recent orders (50)")
    out.append("")
    out.append("| ts | market_id | side | px | size | status | filled |")
    out.append("|---:|---|---|---:|---:|---|---:|")
    for o in orders[:50]:
        out.append(
            f"| {float(o.get('created_ts') or 0.0):.0f} | `{o.get('market_id')}` | {o.get('side')} | {float(o.get('price') or 0.0):.3f} | "
            f"{float(o.get('size') or 0.0):.2f} | {o.get('status')} | {float(o.get('filled_size') or 0.0):.2f} |"
        )
    out.append("")

    if log_tail:
        out.append("## Log tail")
        out.append("")
        out.append("```")
        out.append(log_tail)
        out.append("```")
        out.append("")

    return "\n".join(out).strip() + "\n"


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "superspreader-local-publisher",
    }


def _create_gist(token: str, *, filename: str, content: str) -> GistRef:
    r = requests.post(
        "https://api.github.com/gists",
        headers=_github_headers(token),
        json={
            "description": "SuperSpreader trading snapshots (auto-updated)",
            "public": False,
            "files": {filename: {"content": content}},
        },
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    return GistRef(gist_id=str(data["id"]), html_url=data.get("html_url"))


def _update_gist(token: str, gist_id: str, *, filename: str, content: str) -> None:
    r = requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers=_github_headers(token),
        json={"files": {filename: {"content": content}}},
        timeout=20,
    )
    r.raise_for_status()


async def run_github_publisher_task(settings: Any, store: Any) -> None:
    """
    Optional background task: periodically publish a compact performance snapshot to a GitHub Gist.

    Enable with:
    - GITHUB_PUBLISH_ENABLED=1
    - GH_TOKEN or GITHUB_TOKEN (token must have 'gist' scope if classic PAT)
    - optionally GITHUB_GIST_ID to update an existing gist
    """
    log = get_logger(__name__)

    if not bool(getattr(settings, "github_publish_enabled", False)):
        while True:
            await asyncio.sleep(3600)

    token = getattr(settings, "github_token", None)
    if not token:
        log.warning("github_publish.disabled", reason="missing_token")
        while True:
            await asyncio.sleep(3600)

    gist_id = getattr(settings, "github_gist_id", None)
    interval = int(getattr(settings, "github_publish_interval_secs", 60))
    tail_lines = int(getattr(settings, "github_publish_log_tail_lines", 200))
    log_file = getattr(settings, "log_file", None)
    filename = "snapshot.md"

    while True:
        try:
            tail = None
            if log_file:
                tail = await asyncio.to_thread(_read_tail_lines, str(log_file), tail_lines)
            content = await asyncio.to_thread(_build_report_md, store, log_tail=tail)

            if not gist_id:
                ref = await asyncio.to_thread(_create_gist, token, filename=filename, content=content)
                gist_id = ref.gist_id
                log.info("github_publish.gist_created", gist_id=gist_id, url=ref.html_url)
            else:
                await asyncio.to_thread(_update_gist, token, str(gist_id), filename=filename, content=content)
                log.info("github_publish.updated", gist_id=str(gist_id))
        except Exception:
            log.exception("github_publish.error")

        await asyncio.sleep(max(10, interval))

