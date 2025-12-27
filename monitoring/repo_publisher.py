from __future__ import annotations

import asyncio
import base64
import time
from typing import Any

import requests

from monitoring.github_publisher import _build_report_md, _read_tail_lines
from monitoring.publisher_status import set_publisher_status
from utils.logging import get_logger


def _github_headers(token: str) -> dict[str, str]:
    return {
        # Use Bearer to support both classic and fine-grained tokens.
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "superspreader-local-repo-publisher",
    }


def _raise_for_status_with_context(r: requests.Response, *, action: str) -> None:
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
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
                " Hint: GitHub returned 403 Forbidden. Your token is valid but likely lacks access. For fine-grained "
                "PATs, grant this repo access and set 'Contents: Read and write'."
            )
        raise requests.HTTPError(
            f"{action} failed: HTTP {r.status_code}. body={body!r}{hint}",
            response=r,
            request=getattr(e, "request", None),
        ) from e


def _get_current_file_sha(token: str, repo: str, path: str, branch: str) -> str | None:
    r = requests.get(
        f"https://api.github.com/repos/{repo}/contents/{path}",
        headers=_github_headers(token),
        params={"ref": branch},
        timeout=20,
    )
    if r.status_code == 404:
        return None
    _raise_for_status_with_context(r, action="get_contents_sha")
    data = r.json()
    # If the path resolves to a directory, GitHub returns a list.
    if isinstance(data, list):
        return None
    if isinstance(data, dict):
        return data.get("sha")
    return None


def _put_file(token: str, repo: str, path: str, branch: str, *, content: str, sha: str | None, message: str) -> None:
    b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    payload: dict[str, Any] = {
        "message": message,
        "content": b64,
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(
        f"https://api.github.com/repos/{repo}/contents/{path}",
        headers=_github_headers(token),
        json=payload,
        timeout=30,
    )
    _raise_for_status_with_context(r, action="put_contents_file")


async def run_repo_publisher_task(settings: Any, store: Any) -> None:
    """
    Publish a periodic snapshot into a repo file (creates a commit each update).

    Enable with:
    - GITHUB_REPO_PUBLISH_ENABLED=1
    - GITHUB_REPO=owner/name (e.g. thepaypay420/SuperSpreader)
    - GH_TOKEN or GITHUB_TOKEN with permission to write contents
    - optional: GITHUB_REPO_BRANCH, GITHUB_REPO_PATH
    """
    log = get_logger(__name__)

    if not bool(getattr(settings, "github_repo_publish_enabled", False)):
        set_publisher_status(
            "github_repo",
            state="disabled",
            enabled=False,
            detail={"reason": "not_enabled"},
        )
        log.info(
            "github_repo_publish.disabled",
            reason="not_enabled",
            hint="Set GITHUB_REPO_PUBLISH_ENABLED=1, GITHUB_REPO=owner/name, and GH_TOKEN/GITHUB_TOKEN to publish into a repo file.",
        )
        while True:
            await asyncio.sleep(3600)

    token = getattr(settings, "github_token", None)
    if not token:
        set_publisher_status(
            "github_repo",
            state="disabled",
            enabled=False,
            detail={"reason": "missing_token"},
        )
        log.warning("github_repo_publish.disabled", reason="missing_token")
        while True:
            await asyncio.sleep(3600)

    repo = getattr(settings, "github_repo", None)
    if not repo:
        set_publisher_status(
            "github_repo",
            state="disabled",
            enabled=False,
            detail={"reason": "missing_repo"},
        )
        log.warning("github_repo_publish.disabled", reason="missing_repo")
        while True:
            await asyncio.sleep(3600)

    branch = str(getattr(settings, "github_repo_branch", "main"))
    path = str(getattr(settings, "github_repo_path", "ops/telemetry/latest.md")).lstrip("/")
    prefix = str(getattr(settings, "github_repo_commit_prefix", "telemetry"))
    interval = int(getattr(settings, "github_publish_interval_secs", 60))
    tail_lines = int(getattr(settings, "github_publish_log_tail_lines", 200))
    log_file = getattr(settings, "log_file", None)

    last_sha: str | None = None

    set_publisher_status(
        "github_repo",
        state="running",
        enabled=True,
        detail={
            "repo": str(repo),
            "branch": branch,
            "path": path,
            "url": f"https://github.com/{repo}/blob/{branch}/{path}",
            "interval_secs": interval,
            "has_log_file": bool(log_file),
            "tail_lines": tail_lines,
        },
    )

    log.info(
        "github_repo_publish.enabled",
        repo=str(repo),
        branch=branch,
        path=path,
        interval_secs=interval,
        has_log_file=bool(log_file),
        tail_lines=tail_lines,
    )

    while True:
        attempt_ts = time.time()
        set_publisher_status("github_repo", last_attempt_ts=attempt_ts)
        try:
            tail = None
            if log_file:
                tail = await asyncio.to_thread(_read_tail_lines, str(log_file), tail_lines)
            content = await asyncio.to_thread(_build_report_md, store, log_tail=tail)

            # Refresh SHA occasionally (or first time) in case it changed.
            if last_sha is None:
                last_sha = await asyncio.to_thread(_get_current_file_sha, token, repo, path, branch)

            msg = f"{prefix}: update {path} @ {time.strftime('%Y-%m-%d %H:%M:%S')}"
            await asyncio.to_thread(_put_file, token, repo, path, branch, content=content, sha=last_sha, message=msg)

            # Fetch the new SHA so next update is consistent.
            last_sha = await asyncio.to_thread(_get_current_file_sha, token, repo, path, branch)
            log.info("github_repo_publish.updated", repo=str(repo), branch=branch, path=path)
            set_publisher_status(
                "github_repo",
                state="ok",
                last_success_ts=time.time(),
                last_error=None,
                detail={
                    "repo": str(repo),
                    "branch": branch,
                    "path": path,
                    "url": f"https://github.com/{repo}/blob/{branch}/{path}",
                    "interval_secs": interval,
                    "has_log_file": bool(log_file),
                    "tail_lines": tail_lines,
                },
            )
        except Exception as e:
            # Force re-fetch SHA next loop (handles manual edits / force pushes).
            last_sha = None
            log.exception("github_repo_publish.error", repo=str(repo), branch=branch, path=path)
            set_publisher_status(
                "github_repo",
                state="error",
                last_error=str(e)[:2000],
            )

        await asyncio.sleep(max(15, interval))

