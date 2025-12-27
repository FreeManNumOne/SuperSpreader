from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from utils.logging import get_logger
from monitoring.publisher_status import set_publisher_status


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
    # GitHub supports multiple auth schemes. In practice:
    # - Classic PATs (ghp_...) are most reliably accepted as `Authorization: token <pat>`.
    # - Fine-grained PATs (github_pat_...) and OAuth tokens generally work with Bearer.
    scheme = "token" if token.startswith("ghp_") else "Bearer"
    return {
        "Authorization": f"{scheme} {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "superspreader-local-publisher",
    }


def _raise_for_status_with_context(r: requests.Response, *, action: str) -> None:
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # Include response body to make auth/permissions errors actionable.
        try:
            body = r.text
        except Exception:
            body = None
        hint = ""
        if r.status_code == 401:
            hint = (
                " Hint: GitHub returned 401 Bad credentials. Ensure GH_TOKEN/GITHUB_TOKEN is a valid GitHub token "
                "(not expired/revoked) and is not wrapped in quotes in your .env (e.g. use GH_TOKEN=ghp_xxx, not "
                "GH_TOKEN=\"ghp_xxx\")."
            )
        elif r.status_code == 403:
            hint = (
                " Hint: GitHub returned 403 Forbidden. Your token is valid but likely lacks permission. For classic "
                "PATs, ensure the right scopes; for fine-grained PATs, grant access to the target resource."
            )
        raise requests.HTTPError(
            f"{action} failed: HTTP {r.status_code}. body={body!r}{hint}",
            response=r,
            request=getattr(e, "request", None),
        ) from e


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
    _raise_for_status_with_context(r, action="create_gist")
    data = r.json()
    return GistRef(gist_id=str(data["id"]), html_url=data.get("html_url"))


def _update_gist(token: str, gist_id: str, *, filename: str, content: str) -> None:
    r = requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers=_github_headers(token),
        json={"files": {filename: {"content": content}}},
        timeout=20,
    )
    _raise_for_status_with_context(r, action="update_gist")


def _write_text_file(path: str, content: str) -> None:
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = Path.cwd() / p
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


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
        set_publisher_status(
            "github_gist",
            state="disabled",
            enabled=False,
            detail={"reason": "not_enabled"},
        )
        log.info(
            "github_publish.disabled",
            reason="not_enabled",
            hint="Set GITHUB_PUBLISH_ENABLED=1 and GH_TOKEN/GITHUB_TOKEN to publish a snapshot gist.",
        )
        while True:
            await asyncio.sleep(3600)

    token = getattr(settings, "github_token", None)
    if not token:
        set_publisher_status(
            "github_gist",
            state="disabled",
            enabled=False,
            detail={"reason": "missing_token"},
        )
        log.warning("github_publish.disabled", reason="missing_token")
        while True:
            await asyncio.sleep(3600)

    gist_id = getattr(settings, "github_gist_id", None)
    gist_id_file = getattr(settings, "github_gist_id_file", None)
    interval = int(getattr(settings, "github_publish_interval_secs", 60))
    tail_lines = int(getattr(settings, "github_publish_log_tail_lines", 200))
    log_file = getattr(settings, "log_file", None)
    filename = "snapshot.md"

    set_publisher_status(
        "github_gist",
        state="running",
        enabled=True,
        detail={
            "has_gist_id": bool(gist_id),
            "gist_id_file": str(gist_id_file) if gist_id_file else None,
            "interval_secs": interval,
            "has_log_file": bool(log_file),
            "tail_lines": tail_lines,
        },
    )

    log.info(
        "github_publish.enabled",
        interval_secs=interval,
        has_gist_id=bool(gist_id),
        has_log_file=bool(log_file),
        tail_lines=tail_lines,
    )

    while True:
        attempt_ts = time.time()
        set_publisher_status("github_gist", last_attempt_ts=attempt_ts)
        try:
            tail = None
            if log_file:
                tail = await asyncio.to_thread(_read_tail_lines, str(log_file), tail_lines)
            content = await asyncio.to_thread(_build_report_md, store, log_tail=tail)

            if not gist_id:
                ref = await asyncio.to_thread(_create_gist, token, filename=filename, content=content)
                gist_id = ref.gist_id
                log.info("github_publish.gist_created", gist_id=gist_id, url=ref.html_url)
                set_publisher_status(
                    "github_gist",
                    state="ok",
                    last_success_ts=time.time(),
                    last_error=None,
                    detail={
                        "gist_id": str(gist_id),
                        "url": ref.html_url or f"https://gist.github.com/{gist_id}",
                        "interval_secs": interval,
                        "has_log_file": bool(log_file),
                        "tail_lines": tail_lines,
                    },
                )
                if gist_id_file:
                    try:
                        await asyncio.to_thread(_write_text_file, str(gist_id_file), str(gist_id) + "\n")
                        log.info("github_publish.gist_id_saved", path=str(gist_id_file))
                    except Exception:
                        log.exception("github_publish.gist_id_save_error", path=str(gist_id_file))
            else:
                await asyncio.to_thread(_update_gist, token, str(gist_id), filename=filename, content=content)
                log.info("github_publish.updated", gist_id=str(gist_id))
                set_publisher_status(
                    "github_gist",
                    state="ok",
                    last_success_ts=time.time(),
                    last_error=None,
                    detail={
                        "gist_id": str(gist_id),
                        "url": f"https://gist.github.com/{gist_id}",
                        "interval_secs": interval,
                        "has_log_file": bool(log_file),
                        "tail_lines": tail_lines,
                    },
                )
        except Exception as e:
            log.exception("github_publish.error")
            set_publisher_status(
                "github_gist",
                state="error",
                last_error=str(e)[:2000],
            )

        await asyncio.sleep(max(10, interval))

