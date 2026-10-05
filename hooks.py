"""
ClaudeHopper Hooks — Automation Stubs
==============================

Drop-in automation for ClaudeHopper's request/response pipeline.

Each function is called automatically by the proxy.  Implement the body to
add automation; return the (optionally modified) data dict unchanged for
transparent pass-through.

Execution order:
    Incoming request
        → before_request()          ← modify/drop outgoing requests
        → [interactive inspector]   ← manual review (if enabled)
        → upstream (Claude)
        → after_response()          ← modify/drop upstream responses
        → [interactive inspector]   ← manual review (if enabled)
        → client

    Streaming responses (passthrough mode only):
        → on_streaming_chunk()      ← per SSE line hook

    On upstream errors:
        → on_error()

Dropping an item:
    Set  data["_drop"] = True  to signal ClaudeHopper to drop the item.
    For requests  → client receives HTTP 400.
    For responses → client receives HTTP 204 (empty).

Notes:
  - These hooks run synchronously *before* the interactive inspector, so any
    changes made here are visible in the inspector UI.
  - Keep hooks fast; they block the proxy coroutine for the duration.
  - Import whatever you need; this file is a plain Python module.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

log = logging.getLogger("claudehopper.hooks")


# ---------------------------------------------------------------------------
# Request hook
# ---------------------------------------------------------------------------

async def before_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Called for every request *before* it is forwarded upstream.

    Parameters
    ----------
    request_data : dict
        {
            "id"        : str   — unique request UUID
            "timestamp" : str   — ISO-8601 UTC
            "type"      : "request"
            "method"    : str   — HTTP verb
            "url"       : str   — full upstream URL
            "path"      : str   — URL path component
            "query"     : str   — raw query string
            "headers"   : dict  — mutable HTTP headers
            "body"      : dict | str  — parsed body (JSON dict, or raw str)
            "_raw_body" : bytes — original request body bytes (read-only)
        }

    Returns
    -------
    dict
        The (potentially modified) request_data.
        Set request_data["_drop"] = True to abort the request.

    Examples
    --------
    # Log every model being requested:
    if isinstance(request_data.get("body"), dict):
        log.info("Model requested: %s", request_data["body"].get("model"))

    # Rewrite the model name:
    # if isinstance(request_data.get("body"), dict):
    #     request_data["body"]["model"] = "claude-opus-4-5"

    # Prepend a system prompt:
    # if isinstance(request_data.get("body"), dict):
    #     system = request_data["body"].get("system", "")
    #     request_data["body"]["system"] = "[AUDIT MODE] " + system

    # Drop requests to a specific path:
    # if request_data["path"] == "v1/complete":
    #     request_data["_drop"] = True
    """

    # ── YOUR AUTOMATION CODE HERE ────────────────────────────────────────
    return request_data


# ---------------------------------------------------------------------------
# Response hook
# ---------------------------------------------------------------------------

async def after_response(
    request_data: Dict[str, Any],
    response_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Called for every response *after* it is received from upstream,
    before it is returned to the client.

    Parameters
    ----------
    request_data : dict
        Original request (see before_request).
    response_data : dict
        {
            "id"          : str   — same UUID as request
            "request_id"  : str   — same UUID as request
            "timestamp"   : str   — ISO-8601 UTC
            "type"        : "response" | "streaming_response"
            "status_code" : int   — HTTP status code
            "headers"     : dict  — mutable response headers
            "body"        : dict | str  — parsed body
            "_raw_body"   : bytes — original response bytes (read-only)

            # streaming_response only:
            "chunks"      : list[str]  — raw SSE lines (mutable)
        }

    Returns
    -------
    dict
        The (potentially modified) response_data.
        Set response_data["_drop"] = True to suppress the response (→ 204).

    Examples
    --------
    # Log the model that actually processed the request:
    if isinstance(response_data.get("body"), dict):
        log.info("Model used: %s  tokens: %s",
                 response_data["body"].get("model"),
                 response_data["body"].get("usage"))

    # Redact a sensitive pattern from the response text:
    # import re
    # if isinstance(response_data.get("body"), dict):
    #     body_str = json.dumps(response_data["body"])
    #     body_str = re.sub(r'(sk-ant-[A-Za-z0-9]+)', '[REDACTED]', body_str)
    #     response_data["body"] = json.loads(body_str)

    # Block error responses from reaching the client:
    # if response_data.get("status_code", 200) >= 500:
    #     response_data["_drop"] = True
    """

    # ── YOUR AUTOMATION CODE HERE ────────────────────────────────────────
    return response_data


# ---------------------------------------------------------------------------
# Streaming chunk hook  (passthrough / non-interactive mode only)
# ---------------------------------------------------------------------------

async def on_streaming_chunk(chunk: str, request_data: Dict[str, Any]) -> str:
    """
    Called for each raw SSE line in a *streaming* response when the proxy is
    in passthrough mode (interactive=False or intercept_responses=False).

    In interactive mode, use after_response() instead — it receives all
    buffered chunks at once.

    Parameters
    ----------
    chunk : str
        A single SSE line, e.g.:
            'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}'
            ''   (blank separator line)
            'data: [DONE]'
    request_data : dict
        The originating request (see before_request).

    Returns
    -------
    str
        The (potentially modified) chunk.
        Return ``""`` (empty string) to suppress the chunk entirely.

    Examples
    --------
    # Log each text delta:
    # if chunk.startswith("data:") and chunk != "data: [DONE]":
    #     try:
    #         payload = json.loads(chunk[5:])
    #         delta = payload.get("delta", {})
    #         if delta.get("type") == "text_delta":
    #             log.debug("STREAM text: %s", delta.get("text", ""))
    #     except Exception:
    #         pass

    # Suppress thinking-block chunks:
    # if '"type":"thinking"' in chunk:
    #     return ""
    """

    # ── YOUR AUTOMATION CODE HERE ────────────────────────────────────────
    return chunk


# ---------------------------------------------------------------------------
# Error hook
# ---------------------------------------------------------------------------

async def on_error(request_data: Dict[str, Any], exc: Exception) -> None:
    """
    Called when an upstream request fails (connection error, timeout, etc.).

    Parameters
    ----------
    request_data : dict
        The originating request (see before_request).
    exc : Exception
        The exception raised by httpx.

    Notes
    -----
    - Re-raise (or let the default raise propagate) to let ClaudeHopper return a 502.
    - Return normally to suppress the error; ClaudeHopper will return an empty 502.

    Examples
    --------
    # Log errors to a file:
    # with open("claudehopper_errors.log", "a") as f:
    #     f.write(f"{request_data['id']}  {type(exc).__name__}: {exc}\\n")
    # raise exc
    """

    # ── YOUR AUTOMATION CODE HERE ────────────────────────────────────────
    raise exc
