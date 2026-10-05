#!/usr/bin/env python3
"""
ClaudeHopper
============

Transparent, CLI-interactive proxy for Anthropic's Claude API.

Architecture:
    [Claude Client]
         |
    [Nginx :8081/:443]   ← TLS termination (optional, see nginx/claudehopper.conf)
         |
    [ClaudeHopper Proxy :8082]   ← This service  (CLI inspector on stdout/stdin)
         |
    [api.anthropic.com]

Usage:
    python claudehopper.py [--port 8082] [--upstream https://api.anthropic.com] [--no-interactive]

Environment variables:
    CLAUDEHOPPER_PORT               Listen port (default: 8082)
    CLAUDEHOPPER_HOST               Bind address (default: 0.0.0.0)
    CLAUDEHOPPER_UPSTREAM           Upstream API base URL (default: https://api.anthropic.com)
    CLAUDEHOPPER_INTERACTIVE        Start in interactive/intercept mode (default: true)
    CLAUDEHOPPER_INTERCEPT_TIMEOUT  Seconds before auto-releasing a held item (default: 300)
    CLAUDEHOPPER_LOG_LEVEL          Logging verbosity (default: INFO)
    CLAUDEHOPPER_LOG_FILE            JSONL traffic log path (default: disabled)
    EDITOR                  Editor used for the (m)odify command (default: vi)
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
from collections import OrderedDict
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
import uuid

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import uvicorn

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_API_BASE   = os.getenv("CLAUDEHOPPER_UPSTREAM", "https://api.anthropic.com")
PROXY_HOST        = os.getenv("CLAUDEHOPPER_HOST", "0.0.0.0")
PROXY_PORT        = int(os.getenv("CLAUDEHOPPER_PORT", "8082"))
INTERACTIVE_MODE  = os.getenv("CLAUDEHOPPER_INTERACTIVE", "true").lower() == "true"
INTERCEPT_TIMEOUT = int(os.getenv("CLAUDEHOPPER_INTERCEPT_TIMEOUT", "300"))
LOG_LEVEL         = os.getenv("CLAUDEHOPPER_LOG_LEVEL", "INFO").upper()
EDITOR            = os.getenv("EDITOR", "vi")
LOG_FILE          = os.getenv("CLAUDEHOPPER_LOG_FILE", "")  # path to JSONL traffic log; empty = disabled

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [ClaudeHopper] %(levelname)s  %(message)s",
)
log = logging.getLogger("claudehopper")

# ---------------------------------------------------------------------------
# ANSI helpers (no extra deps)
# ---------------------------------------------------------------------------

class _C:
    """ANSI escape codes — disabled when stdout is not a TTY."""
    _tty = sys.stdout.isatty()
    RESET  = "\033[0m"  if _tty else ""
    BOLD   = "\033[1m"  if _tty else ""
    DIM    = "\033[2m"  if _tty else ""
    RED    = "\033[31m" if _tty else ""
    GREEN  = "\033[32m" if _tty else ""
    YELLOW = "\033[33m" if _tty else ""
    BLUE   = "\033[34m" if _tty else ""
    CYAN   = "\033[36m" if _tty else ""
    WHITE  = "\033[97m" if _tty else ""

_W = 70  # display width


def _hr(char: str = "─") -> str:
    return _C.DIM + char * _W + _C.RESET


def _print_oneliner(entry: dict) -> None:
    """Compact single-line traffic summary printed in passthrough mode."""
    t   = entry.get("type", "")
    eid = entry.get("id", "")[:8]
    if t == "request":
        method = entry.get("method", "?")[:6]
        path   = "/" + entry.get("path", "").lstrip("/")
        print(f"{_C.BLUE}→ REQ {_C.RESET} {_C.YELLOW}{method:<6}{_C.RESET} {path}  {_C.DIM}[{eid}]{_C.RESET}")
    elif t in ("response", "streaming_response"):
        sc       = entry.get("status_code", "?")
        sc_color = _C.GREEN if isinstance(sc, int) and sc < 400 else _C.RED
        tag      = " stream" if t == "streaming_response" else "      "
        print(f"{_C.GREEN}← RES {_C.RESET} {sc_color}{sc}{tag}{_C.RESET}        {_C.DIM}[{eid}]{_C.RESET}")


def _banner(label: str, color: str = _C.CYAN) -> str:
    return _C.BOLD + color + "━" * _W + "\n" + f"  ClaudeHopper  {label}" + _C.RESET


def _pp_body(body: Any, max_lines: int = 60) -> str:
    """Pretty-print a body, truncating if too long."""
    if isinstance(body, (dict, list)):
        text = json.dumps(body, indent=2, default=str)
    elif body is None:
        return _C.DIM + "  (empty)" + _C.RESET
    else:
        text = str(body)
    lines = text.splitlines()
    if len(lines) > max_lines:
        shown = lines[:max_lines]
        shown.append(_C.DIM + f"  … {len(lines) - max_lines} more lines (use (m)odify to see all)" + _C.RESET)
        return "\n".join(shown)
    return text


def _print_request(data: dict) -> None:
    print()
    print(_banner("REQUEST INTERCEPTED", _C.BLUE))
    print(_hr())
    print(f"  {_C.BOLD}ID      {_C.RESET}: {_C.DIM}{data['id']}{_C.RESET}")
    print(f"  {_C.BOLD}Time    {_C.RESET}: {data.get('timestamp','')}")
    print(f"  {_C.BOLD}Method  {_C.RESET}: {_C.YELLOW}{data.get('method','')}{_C.RESET}")
    print(f"  {_C.BOLD}URL     {_C.RESET}: {_C.CYAN}{data.get('url','')}{_C.RESET}")
    print(_hr())
    print(f"  {_C.BOLD}Headers:{_C.RESET}")
    for k, v in (data.get("headers") or {}).items():
        dv = v[:12] + "…" if ("api-key" in k.lower() or "authorization" in k.lower()) and len(v) > 12 else v
        print(f"    {_C.DIM}{k:<28}{_C.RESET}: {dv}")
    print(_hr())
    print(f"  {_C.BOLD}Body:{_C.RESET}")
    print(_pp_body(data.get("body")))
    print(_hr("━"))


def _print_response(data: dict) -> None:
    sc = data.get("status_code", 0)
    sc_color = _C.GREEN if sc < 400 else _C.RED
    print()
    print(_banner("RESPONSE INTERCEPTED", _C.GREEN if sc < 400 else _C.RED))
    print(_hr())
    print(f"  {_C.BOLD}ID          {_C.RESET}: {_C.DIM}{data['id']}{_C.RESET}")
    print(f"  {_C.BOLD}Time        {_C.RESET}: {data.get('timestamp','')}")
    print(f"  {_C.BOLD}Status      {_C.RESET}: {sc_color}{_C.BOLD}{sc}{_C.RESET}")
    print(f"  {_C.BOLD}Type        {_C.RESET}: {data.get('type','')}")
    print(_hr())
    print(f"  {_C.BOLD}Headers:{_C.RESET}")
    for k, v in (data.get("headers") or {}).items():
        print(f"    {_C.DIM}{k:<28}{_C.RESET}: {v}")
    print(_hr())
    print(f"  {_C.BOLD}Body:{_C.RESET}")
    print(_pp_body(data.get("body") or data.get("chunks")))
    print(_hr("━"))


# ---------------------------------------------------------------------------
# Proxy state
# ---------------------------------------------------------------------------

class ProxyState:
    def __init__(self) -> None:
        self.interactive: bool = INTERACTIVE_MODE
        self.intercept_requests: bool  = True
        self.intercept_responses: bool = True

        # Queue of (item_type, data, future) — drained by cli_inspector_task
        self.intercept_queue: asyncio.Queue = asyncio.Queue()

        # id → item dict, for REST release endpoints
        self.pending: OrderedDict[str, dict] = OrderedDict()
        self.futures: Dict[str, asyncio.Future] = {}

        # Ring-buffer traffic log
        self._log: List[dict] = []
        self._log_max = 500

        # Optional JSONL log file handle (set at startup)
        self.log_fh: Optional[Any] = None

    def add_to_log(self, entry: dict) -> None:
        self._log.append(entry)
        if len(self._log) > self._log_max:
            self._log.pop(0)
        # Write to JSONL file
        if self.log_fh is not None:
            try:
                self.log_fh.write(json.dumps(entry, default=str) + "\n")
                self.log_fh.flush()
            except Exception as exc:
                log.warning("Log file write error: %s", exc)
        # One-liner terminal summary (only in passthrough mode;
        # interactive mode already prints the full display)
        if not self.interactive:
            _print_oneliner(entry)

    def recent_log(self, n: int = 50) -> List[dict]:
        return self._log[-n:]


state = ProxyState()


# ---------------------------------------------------------------------------
# Intercept — queues an item and waits for the CLI inspector to resolve it
# ---------------------------------------------------------------------------

async def intercept(item_type: str, data: dict) -> dict:
    """
    Hold a request or response for CLI inspection.
    Blocks the calling request handler until the inspector issues a decision.
    Returns the (potentially modified) data dict.
    data["_drop"] = True signals the proxy to drop the item.
    """
    item_id = data["id"]
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()

    state.pending[item_id] = data
    state.futures[item_id] = future

    await state.intercept_queue.put((item_type, data, future))

    try:
        result = await asyncio.wait_for(asyncio.shield(future), timeout=INTERCEPT_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        log.warning("Intercept timeout for %s %s — auto-releasing unchanged", item_type, item_id[:8])
        return data
    finally:
        state.pending.pop(item_id, None)
        state.futures.pop(item_id, None)


def _safe_item(data: dict) -> dict:
    """Strip non-serialisable bytes before logging/editing."""
    return {k: v for k, v in data.items() if k != "_raw_body"}


# ---------------------------------------------------------------------------
# CLI Inspector — background task that drives the interactive prompt loop
# ---------------------------------------------------------------------------

def _stdin_readline(prompt: str) -> str:
    """Blocking stdin read — run via executor to avoid blocking the event loop."""
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        return ""


def _open_editor(data: dict) -> Optional[dict]:
    """
    Serialise data to a temp file, open $EDITOR, read back and parse.
    Returns the parsed dict on success, None on cancel/parse error.
    """
    content = json.dumps(data, indent=2, default=str)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="claudehopper_", delete=False
    ) as tf:
        tf.write(content)
        tmpfile = tf.name
    try:
        ret = subprocess.call([EDITOR, tmpfile])
        if ret != 0:
            print(_C.YELLOW + f"  Editor exited {ret}; keeping original." + _C.RESET)
            return None
        with open(tmpfile) as f:
            return json.loads(f.read())
    except json.JSONDecodeError as exc:
        print(_C.RED + f"  Invalid JSON after edit: {exc}" + _C.RESET)
        return None
    except Exception as exc:
        print(_C.RED + f"  Editor error: {exc}" + _C.RESET)
        return None
    finally:
        try:
            os.unlink(tmpfile)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# SSE stream parsing / content-block reconstruction
# ---------------------------------------------------------------------------

def _parse_sse_events(chunks: List[str]) -> List[dict]:
    """Parse a list of raw SSE lines into structured event dicts."""
    events: List[dict] = []
    current: dict = {}
    for line in chunks:
        if line.startswith("event:"):
            current["event"] = line[6:].strip()
        elif line.startswith("data:"):
            raw = line[5:].strip()
            try:
                current["data"] = json.loads(raw)
            except Exception:
                current["data_raw"] = raw
        elif line == "" and current:
            events.append(current)
            current = {}
    if current:
        events.append(current)
    return events


def _assemble_content_blocks(events: List[dict]) -> List[dict]:
    """
    Walk parsed SSE events and assemble full content blocks by merging
    incremental deltas.  Returns blocks sorted by index.
    """
    blocks: Dict[int, dict] = {}
    for evt in events:
        data = evt.get("data")
        if not isinstance(data, dict):
            continue
        dtype = data.get("type", "")

        if dtype == "content_block_start":
            idx = data.get("index", 0)
            cb: dict = dict(data.get("content_block", {}))
            cb["_index"] = idx
            cb.setdefault("_text", "")
            cb.setdefault("_thinking", "")
            cb.setdefault("_json_acc", "")
            blocks[idx] = cb

        elif dtype == "content_block_delta":
            idx = data.get("index", 0)
            if idx not in blocks:
                continue
            delta = data.get("delta", {})
            dkind = delta.get("type", "")
            if dkind == "text_delta":
                blocks[idx]["_text"] += delta.get("text", "")
            elif dkind == "thinking_delta":
                blocks[idx]["_thinking"] += delta.get("thinking", "")
            elif dkind == "input_json_delta":
                blocks[idx]["_json_acc"] += delta.get("partial_json", "")

    for blk in blocks.values():
        if blk.get("type") == "tool_use" and blk["_json_acc"]:
            try:
                blk["_input"] = json.loads(blk["_json_acc"])
            except Exception:
                blk["_input"] = blk["_json_acc"]

    return [blocks[i] for i in sorted(blocks.keys())]


def _make_structured_edit(data: dict) -> dict:
    """
    Build a minimal, human-readable edit structure from a ClaudeHopper data dict.
    Strips raw SSE noise; exposes only actionable content fields.
    """
    dtype = data.get("type", "")

    if dtype == "streaming_response":
        events = _parse_sse_events(data.get("chunks", []))
        blocks = _assemble_content_blocks(events)
        out_blocks = []
        for blk in blocks:
            btype = blk.get("type", "")
            entry: dict = {"index": blk["_index"], "type": btype}
            if btype == "text":
                entry["text"] = blk.get("_text", "")
            elif btype == "thinking":
                entry["thinking"] = blk.get("_thinking", "")
            elif btype == "tool_use":
                entry["name"] = blk.get("name", "")
                entry["tool_id"] = blk.get("id", "")
                inp = blk.get("_input", blk.get("_json_acc", ""))
                if isinstance(inp, (dict, list)):
                    entry["input"] = inp
                else:
                    entry["input_raw"] = str(inp)
            out_blocks.append(entry)
        return {
            "_claudehopper_edit_type": "streaming_response",
            "_claudehopper_note": (
                "Edit content_blocks only.  status_code is also editable.  "
                "Chunks are rebuilt automatically — do not add a 'chunks' key here."
            ),
            "status_code": data.get("status_code", 200),
            "content_blocks": out_blocks,
        }

    elif dtype == "request":
        body = data.get("body", {})
        if not isinstance(body, dict):
            return _safe_item(data)
        out_req: dict = {
            "_claudehopper_edit_type": "request",
            "_claudehopper_note": (
                "Edit model, system, messages (and other body keys).  "
                "URL/headers live in _advanced."
            ),
            "model": body.get("model", ""),
            "system": body.get("system", ""),
            "messages": body.get("messages", []),
        }
        for k in ("max_tokens", "temperature", "tools", "tool_choice", "stream"):
            if k in body:
                out_req[k] = body[k]
        out_req["_advanced"] = {
            "method": data.get("method", ""),
            "url": data.get("url", ""),
            "headers": data.get("headers", {}),
        }
        return out_req

    elif dtype == "response":
        return {
            "_claudehopper_edit_type": "response",
            "_claudehopper_note": "Edit body and status_code.  Headers live in _advanced.",
            "status_code": data.get("status_code", 200),
            "body": data.get("body", {}),
            "_advanced": {"headers": data.get("headers", {})},
        }

    return _safe_item(data)


def _apply_structured_edit(original: dict, edited: dict) -> dict:
    """Merge an edited structured dict back onto the original data dict."""
    result = dict(original)
    etype = edited.get("_claudehopper_edit_type", "")

    if etype == "streaming_response":
        edited_blocks = edited.get("content_blocks", [])
        block_map = {b.get("index", i): b for i, b in enumerate(edited_blocks)}
        original_events = _parse_sse_events(original.get("chunks", []))
        new_chunks = _rebuild_sse_chunks(original_events, block_map)
        result["chunks"] = new_chunks
        result["body"] = "\n".join(new_chunks)
        if "status_code" in edited:
            result["status_code"] = edited["status_code"]

    elif etype == "request":
        body = dict(original.get("body") or {})
        for k in ("model", "system", "messages", "max_tokens", "temperature",
                   "tools", "tool_choice", "stream"):
            if k in edited:
                body[k] = edited[k]
        result["body"] = body
        adv = edited.get("_advanced", {})
        if adv.get("headers"):
            result["headers"] = adv["headers"]
        if adv.get("url"):
            result["url"] = adv["url"]
        if adv.get("method"):
            result["method"] = adv["method"]

    elif etype == "response":
        if "body" in edited:
            result["body"] = edited["body"]
        if "status_code" in edited:
            result["status_code"] = edited["status_code"]
        adv = edited.get("_advanced", {})
        if adv.get("headers"):
            result["headers"] = adv["headers"]

    else:
        for k, v in edited.items():
            if not k.startswith("_claudehopper"):
                result[k] = v

    return result


def _rebuild_sse_chunks(original_events: List[dict], block_map: dict) -> List[str]:
    """
    Reconstruct an SSE chunk list from original parsed events, replacing
    content blocks with edited versions from block_map {index → block_dict}.
    """
    out: List[str] = []
    skip_deltas: set = set()

    for evt in original_events:
        data = evt.get("data")
        etype = evt.get("event", "")

        if not isinstance(data, dict):
            _emit_sse_raw(out, evt)
            continue

        dtype = data.get("type", "")

        if dtype == "content_block_start":
            idx = data.get("index", 0)
            if idx in block_map:
                edited = block_map[idx]
                cb = dict(data.get("content_block", {}))
                new_btype = edited.get("type", cb.get("type", ""))
                cb["type"] = new_btype
                if new_btype == "tool_use":
                    cb["name"] = edited.get("name", cb.get("name", ""))
                    cb["id"] = edited.get("tool_id", cb.get("id", ""))
                    cb["input"] = {}
                elif new_btype == "text":
                    # Strip tool_use fields if the block type was changed
                    cb.pop("name", None)
                    cb.pop("id", None)
                    cb.pop("input", None)
                    cb.setdefault("text", "")
                new_start = dict(data)
                new_start["content_block"] = cb
                _emit_sse(out, etype, new_start)
                skip_deltas.add(idx)
                _emit_block_deltas(out, idx, edited)
            else:
                _emit_sse(out, etype, data)

        elif dtype == "content_block_delta":
            idx = data.get("index", 0)
            if idx not in skip_deltas:
                _emit_sse(out, etype, data)

        elif dtype == "content_block_stop":
            idx = data.get("index", 0)
            skip_deltas.discard(idx)
            _emit_sse(out, etype, data)

        else:
            _emit_sse(out, etype, data)

    return out


def _emit_block_deltas(out: List[str], idx: int, blk: dict) -> None:
    """Emit a single SSE delta event for a content block's content."""
    btype = blk.get("type", "")
    if btype == "text":
        text = blk.get("text", "")
        if text:
            _emit_sse(out, "content_block_delta", {
                "type": "content_block_delta", "index": idx,
                "delta": {"type": "text_delta", "text": text},
            })
    elif btype == "thinking":
        thinking = blk.get("thinking", "")
        if thinking:
            _emit_sse(out, "content_block_delta", {
                "type": "content_block_delta", "index": idx,
                "delta": {"type": "thinking_delta", "thinking": thinking},
            })
    elif btype == "tool_use":
        inp = blk.get("input", blk.get("input_raw", {}))
        inp_str = json.dumps(inp) if isinstance(inp, (dict, list)) else str(inp)
        if inp_str and inp_str != "{}":
            _emit_sse(out, "content_block_delta", {
                "type": "content_block_delta", "index": idx,
                "delta": {"type": "input_json_delta", "partial_json": inp_str},
            })


def _sse_lines(event_type: str, data: dict) -> List[str]:
    lines: List[str] = []
    if event_type:
        lines.append(f"event: {event_type}")
    lines.append(f"data: {json.dumps(data, default=str)}")
    lines.append("")
    return lines


def _emit_sse(out: List[str], event_type: str, data: dict) -> None:
    out.extend(_sse_lines(event_type, data))


def _emit_sse_raw(out: List[str], evt: dict) -> None:
    if "event" in evt:
        out.append(f"event: {evt['event']}")
    d = evt.get("data")
    if d is not None:
        out.append(f"data: {json.dumps(d, default=str) if isinstance(d, dict) else d}")
    elif "data_raw" in evt:
        out.append(f"data: {evt['data_raw']}")
    out.append("")


def _open_structured_editor(data: dict) -> Optional[dict]:
    """
    Open $EDITOR with a clean structured representation of the data.
    Returns a modified full data dict on success, None on cancel/error.
    """
    structured = _make_structured_edit(data)
    content = json.dumps(structured, indent=2, default=str)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="claudehopper_edit_", delete=False
    ) as tf:
        tf.write(content)
        tmpfile = tf.name
    try:
        ret = subprocess.call([EDITOR, tmpfile])
        if ret != 0:
            print(_C.YELLOW + f"  Editor exited {ret}; keeping original." + _C.RESET)
            return None
        with open(tmpfile) as f:
            edited_structured = json.loads(f.read())
        result = _apply_structured_edit(data, edited_structured)
        for key in ("id", "_raw_body", "_drop"):
            if key in data and key not in result:
                result[key] = data[key]
        return result
    except json.JSONDecodeError as exc:
        print(_C.RED + f"  Invalid JSON after edit: {exc}" + _C.RESET)
        return None
    except Exception as exc:
        print(_C.RED + f"  Editor error: {exc}" + _C.RESET)
        return None
    finally:
        try:
            os.unlink(tmpfile)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Express (x) command — targeted inline content swapping without an editor
# ---------------------------------------------------------------------------

_TOOL_BASH = frozenset({
    "bash", "bashtool", "computer", "execute_command", "run_command",
    "run_bash", "shell", "terminal",
})
_TOOL_WRITE = frozenset({
    "write", "writefile", "write_file", "createfile", "create_file",
    "str_replace_based_edit_tool", "text_editor", "edit_file",
    "write_to_file", "create_or_overwrite_file",
})


async def _express_edit(
    data: dict,
    item_type: str,
    loop: asyncio.AbstractEventLoop,
) -> Optional[dict]:
    """
    Present detected content (commands, text, file writes) inline and allow
    targeted replacement.  No editor required.
    Returns the modified data dict, or None if no changes were made.
    """
    dtype = data.get("type", "")

    # ── Streaming responses ────────────────────────────────────────────────
    if dtype == "streaming_response":
        events = _parse_sse_events(data.get("chunks", []))
        blocks = _assemble_content_blocks(events)
        if not blocks:
            print(_C.YELLOW + "  No content blocks detected." + _C.RESET)
            return None

        made_changes = False
        block_map: dict = {}

        for blk in blocks:
            btype = blk.get("type", "")
            idx = blk.get("_index", 0)
            eb: dict = {
                "index": idx, "type": btype,
                "name": blk.get("name", ""), "tool_id": blk.get("id", ""),
            }
            # Only blocks that are actually modified go into block_map.
            # Unmodified blocks are left out so their original SSE events
            # (including thinking signature_delta, original partial deltas,
            # etc.) pass through _rebuild_sse_chunks completely unchanged.
            block_changed = False

            if btype == "thinking":
                # Never modified via express edit — always pass through as-is
                # to preserve the signature_delta that _assemble_content_blocks
                # discards (it only captures thinking_delta, not signature_delta).
                print(_C.DIM + f"  [block {idx}] <thinking> — skipped" + _C.RESET)

            elif btype == "text":
                text = blk.get("_text", "")
                if text.strip():
                    print()
                    print(_hr())
                    print(f"  {_C.BOLD}[block {idx}] Text response:{_C.RESET}")
                    print(_pp_body(text, max_lines=20))
                    raw = await loop.run_in_executor(
                        None, _stdin_readline,
                        _C.DIM + "  [r]eplace / [e]ditor / [c]md / [f]ile_write / Enter=skip: " + _C.RESET,
                    )
                    choice = raw.strip().lower()
                    if choice == "r":
                        raw2 = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  New text: " + _C.RESET,
                        )
                        eb["text"] = raw2.rstrip("\n")
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Text replaced." + _C.RESET)
                    elif choice == "e":
                        result = await loop.run_in_executor(
                            None, _open_editor, {"text": text}
                        )
                        new_text = result.get("text", text) if result else text
                        if new_text != text:
                            eb["text"] = new_text
                            block_changed = True
                            made_changes = True
                            print(_C.GREEN + "  → Text modified." + _C.RESET)
                    elif choice == "c":
                        raw2 = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Command: " + _C.RESET,
                        )
                        eb["type"] = "tool_use"
                        eb["name"] = "bash"
                        eb["tool_id"] = f"toolu_{uuid.uuid4().hex[:24]}"
                        eb["input"] = {"command": raw2.rstrip("\n")}
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Converted to cmd." + _C.RESET)
                    elif choice == "f":
                        raw_p = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Path: " + _C.RESET,
                        )
                        raw_c = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Content: " + _C.RESET,
                        )
                        eb["type"] = "tool_use"
                        eb["name"] = "write_file"
                        eb["tool_id"] = f"toolu_{uuid.uuid4().hex[:24]}"
                        eb["input"] = {"path": raw_p.strip(), "content": raw_c.rstrip("\n")}
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Converted to file_write block." + _C.RESET)

            elif btype == "tool_use":
                name = blk.get("name", "")
                inp = blk.get("_input", {})
                if not isinstance(inp, dict):
                    inp = {}
                name_lc = name.lower()
                print()
                print(_hr())
                print(f"  {_C.BOLD}[block {idx}] Tool: {_C.YELLOW}{name}{_C.RESET}")

                is_bash = name_lc in _TOOL_BASH or any(
                    k in name_lc for k in ("bash", "shell", "exec", "command", "run")
                )
                is_write = name_lc in _TOOL_WRITE or any(
                    k in name_lc for k in ("write", "edit", "create", "replace", "overwrite")
                )

                if is_bash:
                    cmd_key = next(
                        (k for k in ("command", "cmd", "input") if k in inp), "command"
                    )
                    cmd = inp.get(cmd_key, "")
                    print(f"  {_C.BOLD}Command:{_C.RESET} {_C.YELLOW}{cmd}{_C.RESET}")
                    trunc = cmd[:57] + "…" if len(cmd) > 60 else cmd
                    raw = await loop.run_in_executor(
                        None, _stdin_readline,
                        _C.DIM + f"  New cmd / [t]ext / [f]ile_write / Enter=keep '{trunc}': " + _C.RESET,
                    )
                    choice = raw.strip()
                    if choice.lower() == "t":
                        raw2 = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Text content: " + _C.RESET,
                        )
                        eb["type"] = "text"
                        eb["text"] = raw2.rstrip("\n")
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Converted to text block." + _C.RESET)
                    elif choice.lower() == "f":
                        raw_p = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Path: " + _C.RESET,
                        )
                        raw_c = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Content: " + _C.RESET,
                        )
                        eb["type"] = "tool_use"
                        eb["name"] = "write_file"
                        eb["input"] = {"path": raw_p.strip(), "content": raw_c.rstrip("\n")}
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Converted to file_write block." + _C.RESET)
                    elif choice:
                        eb["input"] = {**inp, cmd_key: choice}
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + f"  → Command → {choice}" + _C.RESET)

                elif is_write:
                    path_key = next(
                        (k for k in ("path", "file_path", "filename") if k in inp), "path"
                    )
                    content_key = next(
                        (k for k in ("content", "new_content", "new_str", "text")
                         if k in inp), "content"
                    )
                    path = inp.get(path_key, "")
                    content = inp.get(content_key, "")
                    print(f"  {_C.BOLD}Path:   {_C.RESET} {_C.CYAN}{path}{_C.RESET}")
                    if content:
                        print(f"  {_C.BOLD}Content:{_C.RESET}")
                        print(_pp_body(content, max_lines=12))
                    raw_p = await loop.run_in_executor(
                        None, _stdin_readline,
                        _C.DIM + "  New path / [t]ext / [c]md / Enter=keep: " + _C.RESET,
                    )
                    new_p = raw_p.strip()
                    type_overridden = False
                    if new_p.lower() == "t":
                        raw_c = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Text content: " + _C.RESET,
                        )
                        eb["type"] = "text"
                        eb["text"] = raw_c.rstrip("\n")
                        block_changed = True
                        made_changes = True
                        type_overridden = True
                        print(_C.GREEN + "  → Converted to text block." + _C.RESET)
                    elif new_p.lower() == "c":
                        raw_c = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Command: " + _C.RESET,
                        )
                        eb["type"] = "tool_use"
                        eb["name"] = "bash"
                        eb["input"] = {"command": raw_c.rstrip("\n")}
                        block_changed = True
                        made_changes = True
                        type_overridden = True
                        print(_C.GREEN + "  → Converted to cmd." + _C.RESET)
                    if not type_overridden:
                        raw_ca = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.DIM + "  Edit content? [r]eplace / [e]ditor / Enter=keep: " + _C.RESET,
                        )
                        new_inp = dict(inp)
                        if new_p:
                            new_inp[path_key] = new_p
                            block_changed = True
                            print(_C.GREEN + f"  → Path → {new_p}" + _C.RESET)
                        ca = raw_ca.strip().lower()
                        if ca == "r":
                            raw_c = await loop.run_in_executor(
                                None, _stdin_readline,
                                _C.CYAN + "  New content: " + _C.RESET,
                            )
                            new_inp[content_key] = raw_c.rstrip("\n")
                            block_changed = True
                            print(_C.GREEN + "  → Content replaced." + _C.RESET)
                        elif ca == "e":
                            res = await loop.run_in_executor(
                                None, _open_editor, {"content": content, "path": path}
                            )
                            if res:
                                new_inp[content_key] = res.get(
                                    content_key, res.get("content", content)
                                )
                                block_changed = True
                                print(_C.GREEN + "  → Content modified." + _C.RESET)
                        if block_changed:
                            made_changes = True
                            eb["input"] = new_inp

                else:
                    # Generic tool — show summary and offer editor
                    print(f"  {_C.BOLD}Input:{_C.RESET}")
                    print(_pp_body(inp, max_lines=15))
                    raw = await loop.run_in_executor(
                        None, _stdin_readline,
                        _C.DIM + "  Edit input? [t]ext / [e]ditor / Enter=skip: " + _C.RESET,
                    )
                    gchoice = raw.strip().lower()
                    if gchoice == "t":
                        raw2 = await loop.run_in_executor(
                            None, _stdin_readline,
                            _C.CYAN + "  Text content: " + _C.RESET,
                        )
                        eb["type"] = "text"
                        eb["text"] = raw2.rstrip("\n")
                        block_changed = True
                        made_changes = True
                        print(_C.GREEN + "  → Converted to text block." + _C.RESET)
                    elif gchoice == "e":
                        res = await loop.run_in_executor(
                            None, _open_editor, {"input": inp}
                        )
                        if res and "input" in res:
                            eb["input"] = res["input"]
                            block_changed = True
                            made_changes = True
                            print(_C.GREEN + "  → Input modified." + _C.RESET)

            # Only route changed blocks through the rebuild path.
            # Unchanged blocks are absent from block_map so _rebuild_sse_chunks
            # emits their original events verbatim (all partial deltas intact).
            if block_changed:
                block_map[idx] = eb

        if not made_changes:
            print(_C.DIM + "  No changes — keeping original." + _C.RESET)
            return None

        new_chunks = _rebuild_sse_chunks(events, block_map)
        result = dict(data)
        result["chunks"] = new_chunks
        result["body"] = "\n".join(new_chunks)
        return result

    # ── Requests ───────────────────────────────────────────────────────────
    elif item_type == "request":
        body = data.get("body")
        if not isinstance(body, dict):
            print(_C.YELLOW + "  Non-JSON body; use (m)odify instead." + _C.RESET)
            return None

        messages = body.get("messages", [])
        new_messages: list = []
        made_changes = False

        for i, msg in enumerate(messages):
            role = msg.get("role", "?")
            content = msg.get("content")

            if isinstance(content, list):
                new_content: list = []
                for j, blk in enumerate(content):
                    if blk.get("type") == "text":
                        text = blk.get("text", "")
                        if text.strip():
                            print()
                            print(_hr())
                            print(f"  {_C.BOLD}Message [{i}] {role} / text [{j}]:{_C.RESET}")
                            print(_pp_body(text, max_lines=15))
                            raw = await loop.run_in_executor(
                                None, _stdin_readline,
                                _C.DIM + "  Replace? [y/N]: " + _C.RESET,
                            )
                            if raw.strip().lower() == "y":
                                raw2 = await loop.run_in_executor(
                                    None, _stdin_readline,
                                    _C.CYAN + "  New text: " + _C.RESET,
                                )
                                new_content.append({**blk, "text": raw2.rstrip("\n")})
                                made_changes = True
                                print(_C.GREEN + "  → Text replaced." + _C.RESET)
                                continue
                    new_content.append(blk)
                new_messages.append({**msg, "content": new_content})

            elif isinstance(content, str) and content.strip():
                print()
                print(_hr())
                print(f"  {_C.BOLD}Message [{i}] {role}:{_C.RESET}")
                print(_pp_body(content, max_lines=10))
                raw = await loop.run_in_executor(
                    None, _stdin_readline,
                    _C.DIM + "  Replace? [y/N]: " + _C.RESET,
                )
                if raw.strip().lower() == "y":
                    raw2 = await loop.run_in_executor(
                        None, _stdin_readline,
                        _C.CYAN + "  New text: " + _C.RESET,
                    )
                    new_messages.append({**msg, "content": raw2.rstrip("\n")})
                    made_changes = True
                    print(_C.GREEN + "  → Message replaced." + _C.RESET)
                    continue
                new_messages.append(msg)
            else:
                new_messages.append(msg)

        if not made_changes:
            print(_C.DIM + "  No changes — keeping original." + _C.RESET)
            return None

        result = dict(data)
        result["body"] = {**body, "messages": new_messages}
        return result

    else:
        print(_C.YELLOW + f"  Express edit not available for type '{dtype}'; use (m)odify." + _C.RESET)
        return None


_BANNER_ART = r"""
  ____ _                 _      _   _
 / ___| | __ _ _   _  __| | ___| | | | ___  _ __  _ __   ___ _ __
| |   | |/ _` | | | |/ _` |/ _ \ |_| |/ _ \| '_ \| '_ \ / _ \ '__|
| |___| | (_| | |_| | (_| |  __/  _  | (_) | |_) | |_) |  __/ |
 \____|_|\__,_|\__,_|\__,_|\___|_| |_|\___/| .__/| .__/ \___|_|
                                           |_|   |_|
        hop in the middle of Claude's API traffic
"""


_HELP = f"""
  {_C.BOLD}Commands:{_C.RESET}
    {_C.GREEN}r{_C.RESET} / Enter  — Release unchanged
    {_C.CYAN}x{_C.RESET}          — Express swap (command / text / file — no editor needed)
    {_C.CYAN}m{_C.RESET}          — Modify in structured editor (clean content-block view)
    {_C.CYAN}M{_C.RESET}          — Modify raw JSON in $EDITOR ({EDITOR})
    {_C.RED}d{_C.RESET}          — Drop  (request → HTTP 400, response → HTTP 204)
    {_C.YELLOW}p{_C.RESET}          — Release this + disable all future interception
    {_C.YELLOW}P{_C.RESET}          — Toggle intercept on/off (item stays held)
    {_C.DIM}s{_C.RESET}          — Reprint item summary
    {_C.DIM}?{_C.RESET} / h      — Show this help
"""


async def cli_inspector_task() -> None:
    """
    Background task that drains the intercept queue and prompts the user.
    Runs for the lifetime of the process.
    """
    loop = asyncio.get_running_loop()

    while True:
        # ── Passthrough parking loop ───────────────────────────────────────
        # When interception is disabled (via 'p' or REST), park here until
        # the user re-enables it.  stdin is read in an executor so the event
        # loop stays responsive (REST /control endpoint still works).
        while not state.interactive:
            if not sys.stdout.isatty():
                # Non-interactive terminal — just sleep and poll
                await asyncio.sleep(1)
                continue
            print(
                _C.YELLOW + "\n  [PASSTHROUGH] Interception is OFF." + _C.RESET +
                _C.DIM + "  Type 'i' + Enter to re-enable, '?' for help." + _C.RESET
            )
            raw = await loop.run_in_executor(
                None, _stdin_readline,
                _C.DIM + "  [PASSTHROUGH] > " + _C.RESET,
            )
            cmd = raw.strip().lower()
            if cmd == "i":
                state.interactive = True
                state.intercept_requests  = True
                state.intercept_responses = True
                print(_C.GREEN + "  → Interception re-enabled." + _C.RESET)
            elif cmd in ("?", "h"):
                print(_HELP)
            elif state.interactive:
                # Was re-enabled via REST while we were waiting at the prompt
                break
        # ──────────────────────────────────────────────────────────────────

        item_type, data, future = await state.intercept_queue.get()

        if future.done():
            state.intercept_queue.task_done()
            continue

        if item_type == "request":
            _print_request(data)
        else:
            _print_response(data)

        queued_ahead = state.intercept_queue.qsize()
        if queued_ahead:
            print(_C.YELLOW + f"  ({queued_ahead} more item(s) waiting)" + _C.RESET)

        while not future.done():
            try:
                raw = await loop.run_in_executor(
                    None,
                    _stdin_readline,
                    _C.BOLD + _C.WHITE + f"\n  [{item_type[:3].upper()}] r/x/m/M/d/p/? > " + _C.RESET,
                )
            except Exception:
                raw = ""

            cmd = raw.strip()

            if cmd.lower() in ("r", ""):
                print(_C.GREEN + "  → Released unchanged." + _C.RESET)
                future.set_result(data)

            elif cmd.lower() == "x":
                # Express swap — targeted inline prompts, no editor
                print(_C.CYAN + "  → Express edit mode…" + _C.RESET)
                modified = await _express_edit(data, item_type, loop)
                if modified is not None:
                    for key in ("id", "_raw_body", "_drop"):
                        if key in data and key not in modified:
                            modified[key] = data[key]
                    print(_C.GREEN + "  → Released with modifications." + _C.RESET)
                    future.set_result(modified)
                else:
                    print(_C.YELLOW + "  No changes — choose again." + _C.RESET)

            elif cmd.lower() == "m":
                # Structured editor — clean content-block view
                print(_C.CYAN + f"  → Opening {EDITOR} (structured view)…" + _C.RESET)
                modified = await loop.run_in_executor(
                    None, _open_structured_editor, _safe_item(data)
                )
                if modified is not None:
                    for key in ("id", "_raw_body", "_drop"):
                        if key in data and key not in modified:
                            modified[key] = data[key]
                    print(_C.GREEN + "  → Released with modifications." + _C.RESET)
                    future.set_result(modified)
                else:
                    print(_C.YELLOW + "  Modification cancelled — choose again." + _C.RESET)

            elif cmd == "M":
                # Raw JSON editor — original full-data fallback
                print(_C.CYAN + f"  → Opening {EDITOR} (raw JSON)…" + _C.RESET)
                edit_data = _safe_item(data)
                is_stream = edit_data.get("type") == "streaming_response"
                if is_stream:
                    edit_data = {k: v for k, v in edit_data.items() if k != "chunks"}
                modified = await loop.run_in_executor(None, _open_editor, edit_data)
                if modified is not None:
                    for key in ("id", "_raw_body", "_drop"):
                        if key in data and key not in modified:
                            modified[key] = data[key]
                    if is_stream:
                        body_str = modified.get("body", "")
                        modified["chunks"] = (
                            body_str.splitlines()
                            if isinstance(body_str, str)
                            else data.get("chunks", [])
                        )
                    print(_C.GREEN + "  → Released with modifications." + _C.RESET)
                    future.set_result(modified)
                else:
                    print(_C.YELLOW + "  Modification cancelled — choose again." + _C.RESET)

            elif cmd.lower() == "d":
                print(_C.RED + "  → Dropped." + _C.RESET)
                data["_drop"] = True
                future.set_result(data)

            elif cmd.lower() == "p":
                print(_C.YELLOW + "  → Released. Interception disabled." + _C.RESET)
                state.interactive = False
                future.set_result(data)
                # drain remaining queue
                while not state.intercept_queue.empty():
                    try:
                        _, qd, qf = state.intercept_queue.get_nowait()
                        if not qf.done():
                            qf.set_result(qd)
                        state.intercept_queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            elif cmd == "P":
                state.interactive = not state.interactive
                status = "ON" if state.interactive else "OFF"
                print(_C.YELLOW + f"  → Interception toggled {status} (item still held)." + _C.RESET)

            elif cmd.lower() == "s":
                if item_type == "request":
                    _print_request(data)
                else:
                    _print_response(data)

            elif cmd.lower() in ("?", "h"):
                print(_HELP)

            else:
                print(_C.DIM + f"  Unknown command '{cmd}'. Type ? for help." + _C.RESET)

        state.intercept_queue.task_done()


# ---------------------------------------------------------------------------
# Automation hooks (loaded from hooks.py; identity stubs used as fallback)
# ---------------------------------------------------------------------------

try:
    from hooks import before_request, after_response, on_streaming_chunk, on_error  # type: ignore
    log.info("hooks.py loaded — automation stubs active")
except ImportError:
    log.info("hooks.py not found — using identity stubs (no automation)")

    async def before_request(req: dict) -> dict:
        return req

    async def after_response(req: dict, res: dict) -> dict:
        return res

    async def on_streaming_chunk(chunk: str, req: dict) -> str:
        return chunk

    async def on_error(req: dict, exc: Exception) -> None:
        raise exc


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    asyncio.create_task(cli_inspector_task(), name="claudehopper-cli-inspector")
    log.info("CLI inspector task started")
    yield

app = FastAPI(title="ClaudeHopper", docs_url=None, redoc_url=None, lifespan=_lifespan)

# Headers that must not be forwarded between hops
_HOP_BY_HOP = frozenset({
    "host", "content-length", "transfer-encoding", "connection",
    "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "upgrade",
    # Compression: httpx decompresses automatically, so we must not advertise
    # accept-encoding to the upstream (preventing compressed responses) and must
    # not forward content-encoding downstream (body is already decoded).
    "accept-encoding", "content-encoding",
})


# ---------------------------------------------------------------------------
# Optional REST control endpoints (scriptable via curl — no HTML)
# ---------------------------------------------------------------------------

@app.get("/__claudehopper__/state", include_in_schema=False)
async def claudehopper_state() -> dict:
    return {
        "interactive": state.interactive,
        "intercept_requests": state.intercept_requests,
        "intercept_responses": state.intercept_responses,
        "pending_count": len(state.pending),
        "queued_count": state.intercept_queue.qsize(),
        "log_count": len(state._log),
        "upstream": CLAUDE_API_BASE,
    }


@app.get("/__claudehopper__/log", include_in_schema=False)
async def claudehopper_log(limit: int = 50) -> dict:
    return {"log": state.recent_log(limit)}


@app.post("/__claudehopper__/control", include_in_schema=False)
async def claudehopper_control(request: Request) -> dict:
    """Programmatic control — e.g. curl -X POST .../control -d '{\"interactive\":false}'"""
    body = await request.json()
    if "interactive" in body:
        state.interactive = bool(body["interactive"])
    if "intercept_requests" in body:
        state.intercept_requests = bool(body["intercept_requests"])
    if "intercept_responses" in body:
        state.intercept_responses = bool(body["intercept_responses"])
    return {"ok": True, "state": {
        "interactive": state.interactive,
        "intercept_requests": state.intercept_requests,
        "intercept_responses": state.intercept_responses,
    }}


@app.post("/__claudehopper__/release/{item_id}", include_in_schema=False)
async def claudehopper_release(item_id: str, request: Request) -> dict:
    """Release an intercepted item from a script: {"data":{...}} or {"drop":true}"""
    body = await request.json()
    future = state.futures.get(item_id)
    if not future or future.done():
        return Response(
            content=json.dumps({"error": "not found or already released"}),
            status_code=404, media_type="application/json",
        )
    item = state.pending.get(item_id, {})
    if body.get("drop"):
        item["_drop"] = True
        future.set_result(item)
    elif body.get("data"):
        future.set_result(body["data"])
    else:
        future.set_result(item)
    return {"ok": True}


@app.post("/__claudehopper__/release_all", include_in_schema=False)
async def claudehopper_release_all() -> dict:
    """Release all pending items unchanged — useful from scripts."""
    count = 0
    for item_id, future in list(state.futures.items()):
        if not future.done():
            future.set_result(state.pending.get(item_id, {}))
            count += 1
    while not state.intercept_queue.empty():
        try:
            _, qd, qf = state.intercept_queue.get_nowait()
            if not qf.done():
                qf.set_result(qd)
            state.intercept_queue.task_done()
        except asyncio.QueueEmpty:
            break
    return {"ok": True, "released": count}


# ---------------------------------------------------------------------------
# Streaming helpers
# ---------------------------------------------------------------------------

async def _proxy_stream_passthrough(
    method: str,
    url: str,
    headers: dict,
    body: bytes,
    request_data: dict,
) -> StreamingResponse:
    """
    Stream the upstream SSE response directly to the client.
    The httpx client is owned *inside* the generator so it stays open
    for the full duration of the lazily-consumed StreamingResponse.
    """
    async def generate() -> AsyncGenerator[bytes, None]:
        # Client must be created here — StreamingResponse iterates this
        # generator *after* the caller has returned, so any client created
        # in the caller would already be closed.
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0), verify=True) as _client:
            try:
                async with _client.stream(method, url, headers=headers, content=body) as resp:
                    async for line in resp.aiter_lines():
                        line = await on_streaming_chunk(line, request_data)
                        if line or line == "":
                            yield (line + "\n").encode()
            except Exception as exc:
                await on_error(request_data, exc)

    return StreamingResponse(
        generate(),
        status_code=200,
        media_type="text/event-stream",
        headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
    )


async def _proxy_stream_interactive(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict,
    body: bytes,
    request_data: dict,
) -> Response:
    """
    Buffer a complete streaming response, present it to the inspector,
    then replay the (potentially modified) chunks back to the client.
    """
    chunks: List[str] = []
    upstream_status = 200
    upstream_headers: dict = {}

    try:
        async with client.stream(method, url, headers=headers, content=body) as resp:
            upstream_status = resp.status_code
            upstream_headers = dict(resp.headers)
            async for line in resp.aiter_lines():
                chunks.append(line)
    except Exception as exc:
        await on_error(request_data, exc)
        return Response(
            content=json.dumps({"error": "upstream stream error"}).encode(),
            status_code=502,
            media_type="application/json",
        )

    response_data: dict = {
        "id": request_data["id"],
        "request_id": request_data["id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "streaming_response",
        "status_code": upstream_status,
        "headers": upstream_headers,
        "chunks": chunks,
        "body": "\n".join(chunks),
    }

    # Automation hook
    response_data = await after_response(request_data, response_data)

    # Interactive inspection
    if state.intercept_responses:
        response_data = await intercept("response", response_data)

    if response_data.get("_drop"):
        return Response(content=b"", status_code=204)

    replay_chunks: List[str] = response_data.get("chunks", chunks)
    replay_status: int = response_data.get("status_code", upstream_status)
    replay_headers = {
        k: v for k, v in response_data.get("headers", upstream_headers).items()
        if k.lower() not in _HOP_BY_HOP | {"content-length"}
    }

    async def replay() -> AsyncGenerator[bytes, None]:
        for line in replay_chunks:
            yield (line + "\n").encode()

    log_entry = _safe_item(response_data)
    state.add_to_log(log_entry)

    return StreamingResponse(
        replay(),
        status_code=replay_status,
        media_type="text/event-stream",
        headers={**replay_headers, "cache-control": "no-cache", "x-accel-buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Main catch-all proxy route
# ---------------------------------------------------------------------------

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
    include_in_schema=False,
)
async def proxy(request: Request, path: str) -> Response:
    if path.startswith("__claudehopper__"):
        return Response(content=b"reserved", status_code=400)

    # Build upstream URL
    qs = f"?{request.url.query}" if request.url.query else ""
    target_url = f"{CLAUDE_API_BASE}/{path}{qs}"

    # Strip hop-by-hop headers before forwarding
    req_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }

    raw_body = await request.body()

    # Parse body for inspection; keep raw bytes for forwarding
    body_parsed: Any
    try:
        body_parsed = json.loads(raw_body) if raw_body else {}
    except Exception:
        body_parsed = raw_body.decode("utf-8", errors="replace")

    request_data: dict = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "request",
        "method": request.method,
        "url": target_url,
        "path": path,
        "query": request.url.query,
        "headers": req_headers,
        "body": body_parsed,
        "_raw_body": raw_body,
    }

    # ── Automation hook ────────────────────────────────────────────────────
    request_data = await before_request(request_data)

    # ── Interactive request inspection ────────────────────────────────────
    if state.interactive and state.intercept_requests:
        request_data = await intercept("request", request_data)

    if request_data.get("_drop"):
        return Response(
            content=json.dumps({"error": "request dropped by ClaudeHopper proxy"}).encode(),
            status_code=400,
            media_type="application/json",
        )

    # Re-serialise body (may have been modified by hook or inspector)
    send_body: bytes
    if isinstance(request_data.get("body"), dict):
        send_body = json.dumps(request_data["body"]).encode()
    elif isinstance(request_data.get("body"), str):
        send_body = request_data["body"].encode()
    else:
        send_body = request_data.get("_raw_body", raw_body)

    is_streaming = (
        isinstance(request_data.get("body"), dict)
        and bool(request_data["body"].get("stream"))
    )

    state.add_to_log(_safe_item(request_data))

    fwd_headers: dict = request_data.get("headers", req_headers)

    # ── Streaming passthrough — client lives inside the generator ─────────
    # Must be handled BEFORE the async-with block below; returning a
    # StreamingResponse from inside async-with would close the client
    # before Starlette starts iterating the response body.
    if is_streaming and not (state.interactive and state.intercept_responses):
        return await _proxy_stream_passthrough(
            request_data["method"], request_data["url"],
            fwd_headers, send_body, request_data,
        )

    # ── Non-streaming + streaming-interactive (full body buffered) ────────
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0), verify=True) as client:
            if is_streaming:  # interactive + intercept_responses
                return await _proxy_stream_interactive(
                    client, request_data["method"], request_data["url"],
                    fwd_headers, send_body, request_data,
                )

            upstream_resp = await client.request(
                method=request_data["method"],
                url=request_data["url"],
                headers=fwd_headers,
                content=send_body,
            )

    except httpx.RequestError as exc:
        log.error("Upstream error: %s", exc)
        await on_error(request_data, exc)
        return Response(
            content=json.dumps({"error": str(exc), "type": "proxy_upstream_error"}).encode(),
            status_code=502,
            media_type="application/json",
        )

    # Parse upstream response body
    resp_ct = upstream_resp.headers.get("content-type", "")
    resp_body: Any
    if "application/json" in resp_ct:
        try:
            resp_body = upstream_resp.json()
        except Exception:
            resp_body = upstream_resp.text
    else:
        resp_body = upstream_resp.text

    response_data: dict = {
        "id": request_data["id"],
        "request_id": request_data["id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "response",
        "status_code": upstream_resp.status_code,
        "headers": dict(upstream_resp.headers),
        "body": resp_body,
        "_raw_body": upstream_resp.content,
    }

    # ── Automation hook ────────────────────────────────────────────────────
    response_data = await after_response(request_data, response_data)

    # ── Interactive response inspection ───────────────────────────────────
    if state.interactive and state.intercept_responses:
        response_data = await intercept("response", response_data)

    if response_data.get("_drop"):
        return Response(content=b"", status_code=204)

    # Re-serialise response body (may have been modified)
    resp_body_out: bytes
    if isinstance(response_data.get("body"), dict):
        resp_body_out = json.dumps(response_data["body"]).encode()
        resp_ct = "application/json"
    elif isinstance(response_data.get("body"), str):
        resp_body_out = response_data["body"].encode()
    else:
        resp_body_out = response_data.get("_raw_body", upstream_resp.content)

    resp_headers_out = {
        k: v for k, v in response_data.get("headers", {}).items()
        if k.lower() not in _HOP_BY_HOP | {"content-length"}
    }

    state.add_to_log(_safe_item(response_data))

    return Response(
        content=resp_body_out,
        status_code=response_data.get("status_code", upstream_resp.status_code),
        headers=resp_headers_out,
        media_type=resp_ct,
    )


# ---------------------------------------------------------------------------
# (Web inspector removed — CLI-only mode)
# ---------------------------------------------------------------------------

_REMOVED = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ClaudeHopper Inspector</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg:       #0d1117;  --surface:  #161b22;  --border:   #21262d;
    --text:     #c9d1d9;  --muted:    #6e7681;  --accent:   #e94560;
    --blue:     #58a6ff;  --green:    #3fb950;  --yellow:   #d29922;
    --red:      #f85149;  --pending:  #d29922;
  }
  body { font-family: 'Cascadia Code', 'Consolas', monospace; background: var(--bg);
         color: var(--text); height: 100vh; display: flex; flex-direction: column;
         font-size: 13px; }

  /* ── Header ─────────────────────────────────────────────────────────── */
  header { background: var(--surface); padding: 8px 14px; display: flex;
           align-items: center; gap: 12px; border-bottom: 1px solid var(--border);
           flex-shrink: 0; }
  header h1 { font-size: 15px; color: var(--accent); letter-spacing: 3px; }
  .badge { padding: 2px 8px; border-radius: 10px; font-size: 10px; font-weight: 700;
           text-transform: uppercase; }
  .badge.green  { background: #1a3a1a; color: var(--green); }
  .badge.red    { background: #3a1a1a; color: var(--red); }
  .badge.yellow { background: #3a2a00; color: var(--yellow); }
  .spacer { flex: 1; }
  .controls { display: flex; gap: 8px; align-items: center; }
  label.toggle { display: flex; align-items: center; gap: 5px; font-size: 11px;
                 cursor: pointer; color: var(--muted); user-select: none; }
  label.toggle input { cursor: pointer; accent-color: var(--accent); }
  button { padding: 4px 12px; border-radius: 4px; border: 1px solid var(--border);
           cursor: pointer; font-size: 11px; font-family: inherit;
           background: var(--surface); color: var(--text); }
  button:hover { border-color: var(--accent); color: var(--accent); }
  button.danger  { border-color: var(--red);    color: var(--red);    }
  button.success { border-color: var(--green);  color: var(--green);  }
  button.primary { border-color: var(--accent); color: var(--accent); }

  /* ── Main layout ─────────────────────────────────────────────────────── */
  .main { display: flex; flex: 1; overflow: hidden; }

  /* ── Sidebar ─────────────────────────────────────────────────────────── */
  .sidebar { width: 340px; min-width: 220px; display: flex; flex-direction: column;
             border-right: 1px solid var(--border); background: var(--surface); }
  .sidebar-hdr { padding: 7px 12px; font-size: 10px; color: var(--muted);
                 border-bottom: 1px solid var(--border); display: flex;
                 justify-content: space-between; align-items: center; text-transform: uppercase; }
  .item-list { flex: 1; overflow-y: auto; }
  .item { padding: 9px 12px; border-bottom: 1px solid var(--border); cursor: pointer;
          border-left: 3px solid transparent; transition: background 0.1s; }
  .item:hover { background: #1c2128; }
  .item.selected { background: #1c2128; border-left-color: var(--accent); }
  .item.pending  { border-left-color: var(--yellow); }
  .item.released { border-left-color: var(--green); opacity: 0.75; }
  .item.dropped  { border-left-color: var(--red);   opacity: 0.45; }
  .item-row1 { display: flex; gap: 6px; align-items: center; margin-bottom: 3px; }
  .item-method { font-size: 10px; font-weight: 700; color: var(--blue); }
  .item-sc     { font-size: 10px; font-weight: 700; }
  .item-sc.ok  { color: var(--green); }
  .item-sc.err { color: var(--red); }
  .tag { font-size: 9px; padding: 1px 5px; border-radius: 8px; border: 1px solid var(--border); }
  .tag.req   { color: var(--blue); }
  .tag.res   { color: var(--green); }
  .tag.strm  { color: var(--yellow); }
  .pulse { animation: pulse 1.2s infinite; color: var(--yellow); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.2} }
  .item-time { font-size: 9px; color: var(--muted); margin-left: auto; }
  .item-path { font-size: 11px; color: var(--muted); white-space: nowrap;
               overflow: hidden; text-overflow: ellipsis; }

  /* ── Detail pane ─────────────────────────────────────────────────────── */
  .detail { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  .detail-hdr { padding: 8px 14px; background: var(--surface);
                border-bottom: 1px solid var(--border); display: flex;
                gap: 8px; align-items: center; flex-shrink: 0; }
  .detail-title { font-size: 12px; color: var(--text); flex: 1;
                  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .detail-body { flex: 1; display: flex; overflow: hidden; }
  .panel { flex: 1; display: flex; flex-direction: column; overflow: hidden;
           min-width: 0; }
  .panel + .panel { border-left: 1px solid var(--border); }
  .panel-hdr { padding: 5px 12px; font-size: 10px; color: var(--muted);
               background: var(--surface); border-bottom: 1px solid var(--border);
               display: flex; align-items: center; gap: 8px;
               text-transform: uppercase; flex-shrink: 0; }
  .panel-hdr span { flex: 1; }
  .panel-hdr button { padding: 2px 7px; font-size: 10px; }
  .editor { flex: 1; overflow: auto; padding: 10px; }
  textarea { width: 100%; height: 100%; background: var(--bg); color: #79c0ff;
             border: 1px solid var(--border); border-radius: 4px; padding: 10px;
             font-family: inherit; font-size: 12px; resize: none; outline: none;
             line-height: 1.6; }
  textarea:focus { border-color: var(--accent); }
  textarea[readonly] { color: var(--muted); }

  /* ── Empty / Status bar ─────────────────────────────────────────────── */
  .empty { flex: 1; display: flex; align-items: center; justify-content: center;
           color: var(--muted); }
  .statusbar { padding: 3px 14px; background: var(--surface); font-size: 10px;
               color: var(--muted); display: flex; gap: 16px; flex-shrink: 0;
               border-top: 1px solid var(--border); }
  .dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block;
         margin-right: 4px; vertical-align: middle; }
  .dot.on  { background: var(--green); }
  .dot.off { background: var(--red); }
</style>
</head>
<body>

<header>
  <h1>ClaudeHopper</h1>
  <span id="conn-badge" class="badge red">Disconnected</span>
  <span id="mode-badge" class="badge yellow">Interactive</span>
  <div class="spacer"></div>
  <div class="controls">
    <label class="toggle"><input type="checkbox" id="chk-interactive" checked
      onchange="send({action:'set_interactive',         value:this.checked})"> Intercept</label>
    <label class="toggle"><input type="checkbox" id="chk-req" checked
      onchange="send({action:'set_intercept_requests',  value:this.checked})"> Requests</label>
    <label class="toggle"><input type="checkbox" id="chk-res" checked
      onchange="send({action:'set_intercept_responses', value:this.checked})"> Responses</label>
    <button onclick="send({action:'release_all'})">Release All</button>
    <button class="danger" onclick="clearAll()">Clear</button>
  </div>
</header>

<div class="main">
  <div class="sidebar">
    <div class="sidebar-hdr">
      <span>Traffic</span>
      <span id="pending-badge" class="badge yellow" style="display:none">0 pending</span>
    </div>
    <div class="item-list" id="item-list"></div>
  </div>
  <div class="detail" id="detail">
    <div class="empty">Select an item to inspect</div>
  </div>
</div>

<div class="statusbar">
  <span><span class="dot off" id="ws-dot"></span>WebSocket</span>
  <span id="stat-counts">0 req · 0 res</span>
  <span id="stat-upstream"></span>
</div>

<script>
'use strict';
const items = new Map();   // id → item
let selectedId = null;
let ws = null;
let reqN = 0, resN = 0;

// ── WebSocket ──────────────────────────────────────────────────────────────
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/__claudehopper__/ws`);

  ws.onopen = () => {
    document.getElementById('conn-badge').textContent = 'Connected';
    document.getElementById('conn-badge').className = 'badge green';
    document.getElementById('ws-dot').className = 'dot on';
  };
  ws.onclose = () => {
    document.getElementById('conn-badge').textContent = 'Disconnected';
    document.getElementById('conn-badge').className = 'badge red';
    document.getElementById('ws-dot').className = 'dot off';
    setTimeout(connect, 2500);
  };
  ws.onmessage = e => handle(JSON.parse(e.data));
}

function send(obj) { ws && ws.readyState === 1 && ws.send(JSON.stringify(obj)); }

// ── Message handler ────────────────────────────────────────────────────────
function handle(msg) {
  switch (msg.event) {
    case 'connected':
      applyState(msg.state);
      document.getElementById('stat-upstream').textContent = 'Upstream: ' + (msg.upstream || '');
      (msg.pending || []).forEach(i => upsert(i, 'pending'));
      (msg.log || []).forEach(i => { if (!items.has(i.id)) upsert(i, 'released'); });
      break;

    case 'intercept':
      upsert(msg.item, 'pending');
      break;

    case 'request':
      if (!items.has(msg.item.id)) { upsert(msg.item, 'released'); reqN++; updateCounts(); }
      break;

    case 'response':
      upsert(msg.item, 'released'); resN++; updateCounts();
      break;

    case 'released':
      setStatus(msg.id, 'released');
      break;

    case 'released_all':
      items.forEach((v,k) => { if (v._status === 'pending') v._status = 'released'; });
      renderList();
      break;

    case 'dropped':
      setStatus(msg.id, 'dropped');
      break;

    case 'state_update':
      applyState(msg);
      break;
  }
  updatePending();
}

// ── State helpers ──────────────────────────────────────────────────────────
function upsert(item, status) {
  const existing = items.get(item.id);
  if (existing) { existing._status = status; Object.assign(existing, item); }
  else           { items.set(item.id, { ...item, _status: status }); }
  renderList();
  if (selectedId === item.id) renderDetail(items.get(item.id));
}

function setStatus(id, status) {
  const it = items.get(id);
  if (it) { it._status = status; renderList(); if (selectedId === id) renderDetail(it); }
}

function applyState(s) {
  if ('interactive'          in s) document.getElementById('chk-interactive').checked = s.interactive;
  if ('intercept_requests'   in s) document.getElementById('chk-req').checked = s.intercept_requests;
  if ('intercept_responses'  in s) document.getElementById('chk-res').checked = s.intercept_responses;
  if ('interactive' in s) {
    const el = document.getElementById('mode-badge');
    el.textContent = s.interactive ? 'Interactive' : 'Passthrough';
    el.className   = 'badge ' + (s.interactive ? 'yellow' : 'green');
  }
}

function updateCounts() {
  document.getElementById('stat-counts').textContent = `${reqN} req · ${resN} res`;
}
function updatePending() {
  const n = [...items.values()].filter(i => i._status === 'pending').length;
  const el = document.getElementById('pending-badge');
  el.textContent = `${n} pending`;
  el.style.display = n ? '' : 'none';
}

// ── List rendering ─────────────────────────────────────────────────────────
function renderList() {
  const entries = [...items.values()].reverse();
  document.getElementById('item-list').innerHTML = entries.map(it => {
    const t   = it.timestamp ? new Date(it.timestamp).toLocaleTimeString() : '';
    const sel = selectedId === it.id ? ' selected' : '';
    const pend = it._status === 'pending' ? ' <span class="pulse">●</span>' : '';
    const typeTag = it.type === 'streaming_response' ? `<span class="tag strm">stream</span>`
                  : it.type === 'request'            ? `<span class="tag req">req</span>`
                  :                                    `<span class="tag res">res</span>`;
    const left = it.type === 'request'
      ? `<span class="item-method">${it.method||'?'}</span>`
      : `<span class="item-sc ${it.status_code < 400 ? 'ok':'err'}">${it.status_code}</span>`;
    return `<div class="item ${it._status}${sel}" onclick="select('${it.id}')">
      <div class="item-row1">${left}${typeTag}${pend}<span class="item-time">${t}</span></div>
      <div class="item-path">${it.path||it.url||''}</div>
    </div>`;
  }).join('');
}

// ── Detail rendering ───────────────────────────────────────────────────────
function select(id) { selectedId = id; renderList(); renderDetail(items.get(id)); }

function renderDetail(it) {
  if (!it) return;
  const pending = it._status === 'pending';
  const bodyVal = JSON.stringify(it.body ?? it.chunks ?? null, null, 2);
  const hdrVal  = JSON.stringify(it.headers || {}, null, 2);
  const actionBtns = pending
    ? `<button class="success" onclick="releaseItem('${it.id}')">Release</button>
       <button class="primary" onclick="releaseModified('${it.id}')">Release Modified</button>
       <button class="danger"  onclick="dropItem('${it.id}')">Drop</button>`
    : `<span style="font-size:11px;color:var(--${it._status==='dropped'?'red':'green'})">${it._status}</span>`;

  document.getElementById('detail').innerHTML = `
    <div class="detail-hdr">
      <span class="detail-title">${(it.type||'').toUpperCase()} — ${it.id}</span>
      ${actionBtns}
    </div>
    <div class="detail-body">
      <div class="panel">
        <div class="panel-hdr"><span>Headers</span>
          ${pending?`<button onclick="fmt('editor-headers')">Format</button>`:''}
        </div>
        <div class="editor">
          <textarea id="editor-headers" ${pending?'':'readonly'}>${esc(hdrVal)}</textarea>
        </div>
      </div>
      <div class="panel">
        <div class="panel-hdr"><span>Body / Chunks</span>
          ${pending?`<button onclick="fmt('editor-body')">Format</button>`:''}
        </div>
        <div class="editor">
          <textarea id="editor-body" ${pending?'':'readonly'}>${esc(bodyVal)}</textarea>
        </div>
      </div>
    </div>`;
}

function esc(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function fmt(id) {
  const ta = document.getElementById(id);
  try { ta.value = JSON.stringify(JSON.parse(ta.value), null, 2); } catch(_) {}
}

// ── Actions ────────────────────────────────────────────────────────────────
function releaseItem(id)     { send({ action:'release', id }); }
function dropItem(id)        { send({ action:'drop',    id }); }
function releaseModified(id) {
  const it = { ...items.get(id) };
  try { it.headers = JSON.parse(document.getElementById('editor-headers').value); } catch(_) {}
  try { it.body    = JSON.parse(document.getElementById('editor-body').value);    } catch(_) {
    it.body = document.getElementById('editor-body').value;
  }
  send({ action:'release', id, data: it });
}
function clearAll() {
  items.clear(); reqN = 0; resN = 0; selectedId = null;
  renderList(); updateCounts(); updatePending();
  document.getElementById('detail').innerHTML = '<div class="empty">Select an item to inspect</div>';
}

connect();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ClaudeHopper — transparent Claude API intercept proxy",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host",       default=PROXY_HOST,      help="Bind address")
    p.add_argument("--port",       type=int, default=PROXY_PORT, help="Listen port")
    p.add_argument("--upstream",   default=CLAUDE_API_BASE,  help="Upstream API base URL")
    p.add_argument("--no-interactive", action="store_true",  help="Start in passthrough mode")
    p.add_argument("--log-file",   default=LOG_FILE,
                   metavar="PATH",  help="Append all traffic to this JSONL file (one JSON object per line)")
    p.add_argument("--log-level",  default=LOG_LEVEL,
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    CLAUDE_API_BASE = args.upstream  # allow CLI override

    if args.no_interactive:
        state.interactive = False

    if args.log_file:
        try:
            state.log_fh = open(args.log_file, "a", encoding="utf-8")
            log.info("Traffic log → %s", args.log_file)
        except OSError as exc:
            log.error("Cannot open log file %s: %s", args.log_file, exc)

    display_host = args.host if args.host != "0.0.0.0" else "localhost"
    print()
    print(_C.BOLD + _C.CYAN + _BANNER_ART + _C.RESET)
    print(_C.DIM + f"  Proxy    : http://{display_host}:{args.port}" + _C.RESET)
    print(_C.DIM + f"  Upstream : {CLAUDE_API_BASE}" + _C.RESET)
    print(_C.DIM + f"  Mode     : {'interactive (CLI)' if state.interactive else 'passthrough'}" + _C.RESET)
    print(_C.DIM + f"  Editor   : {EDITOR}  (set $EDITOR to change)" + _C.RESET)
    if args.log_file:
        print(_C.DIM + f"  Log file : {args.log_file}" + _C.RESET)
    print()
    print(_HELP)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
        access_log=False,   # ClaudeHopper has its own request logging
    )
