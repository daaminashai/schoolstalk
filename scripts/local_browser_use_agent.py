#!/usr/bin/env python3
"""Small stdio bridge for running Browser Use locally.

The TypeScript app keeps one of these processes alive per scrape session so the
same local Chromium instance can be reused across classify/directory/extract
tasks without Browser Use Cloud.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Union

PREFIX = "__SCHOOLYANK_BROWSER_USE__"


def emit(payload: dict[str, Any]) -> None:
    print(PREFIX + json.dumps(payload, separators=(",", ":")), flush=True)


def die(message: str) -> None:
    emit({"type": "error", "message": message})
    raise SystemExit(1)


try:
    from browser_use import Agent, Browser
    from browser_use.llm.openrouter.chat import ChatOpenRouter
    from pydantic import BaseModel, Field, create_model
except Exception as exc:  # pragma: no cover - exercised only on missing deps
    die(
        "local Browser Use dependencies are missing. Run: "
        "python3 -m pip install -r requirements.txt && browser-use install. "
        "If browser-use install says uvx is missing, rerun the pip install command. "
        f"Import error: {exc}"
    )


OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_OPENROUTER_MODEL = "openai/gpt-5-mini"
DEFAULT_BROWSER_KILL_TIMEOUT_SECONDS = 10.0


def env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_str(name: str) -> str:
    return os.getenv(name, "").strip()


def is_openrouter_base_url(base_url: str) -> bool:
    return base_url.rstrip("/") == OPENROUTER_BASE_URL.rstrip("/") or "openrouter.ai" in base_url


def openrouter_base_url() -> str:
    configured = env_str("AI_BASE_URL")
    return configured if configured and is_openrouter_base_url(configured) else OPENROUTER_BASE_URL


def openrouter_api_key() -> str:
    explicit = env_str("OPENROUTER_API_KEY")
    if explicit:
        return explicit

    legacy = env_str("AI_API_KEY")
    if legacy and is_openrouter_base_url(env_str("AI_BASE_URL")):
        return legacy
    return ""


def openrouter_headers() -> dict[str, str]:
    return {
        "HTTP-Referer": env_str("OPENROUTER_SITE_URL") or "https://github.com/Hex-4/schoolyank",
        "X-OpenRouter-Title": env_str("OPENROUTER_APP_NAME") or "schoolyank",
    }


def local_model(slot: str | None) -> str:
    if slot == "extract":
        return (
            env_str("BROWSER_USE_EXTRACT_MODEL")
            or env_str("BROWSER_USE_LOCAL_MODEL")
            or env_str("AI_MODEL")
            or DEFAULT_OPENROUTER_MODEL
        )
    return (
        env_str("BROWSER_USE_DEFAULT_MODEL")
        or env_str("BROWSER_USE_LOCAL_MODEL")
        or env_str("AI_MODEL")
        or DEFAULT_OPENROUTER_MODEL
    )


def make_llm(slot: str | None) -> ChatOpenRouter:
    api_key = openrouter_api_key()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for local Browser Use")
    return ChatOpenRouter(
        model=local_model(slot),
        api_key=api_key,
        base_url=openrouter_base_url(),
        default_headers=openrouter_headers(),
    )


def isolated_user_data_dir(configured: str) -> tuple[str, str]:
    """Return a per-process profile dir so parallel Chromium launches never share one."""

    session_id = env_str("SCHOOLYANK_BROWSER_SESSION_ID") or "local"
    temp_root = Path(tempfile.mkdtemp(prefix=f"schoolyank-browser-{session_id}-"))

    if not configured:
        return str(temp_root), str(temp_root)

    configured_path = Path(configured).expanduser()
    if not configured_path.is_absolute():
        configured_path = Path.cwd() / configured_path

    # Treat BROWSER_USE_USER_DATA_DIR as a template for single-session runs, but
    # never hand the same user-data-dir to two Chromium processes in parallel.
    default_profile = configured_path / "Default"
    if default_profile.exists():
        try:
            shutil.copytree(default_profile, temp_root / "Default", dirs_exist_ok=True)
        except Exception:
            pass
    local_state = configured_path / "Local State"
    if local_state.exists():
        try:
            shutil.copy2(local_state, temp_root / "Local State")
        except Exception:
            pass

    return str(temp_root), str(temp_root)


def make_browser() -> tuple[Browser, str]:
    kwargs: dict[str, Any] = {"keep_alive": True}
    headless = env_bool("BROWSER_USE_HEADLESS")
    if headless is None:
        headless = env_bool("BROWSER_USE_LOCAL_HEADLESS")
    if headless is not None:
        kwargs["headless"] = headless

    isolated_dir, cleanup_dir = isolated_user_data_dir(os.getenv("BROWSER_USE_USER_DATA_DIR", ""))
    kwargs["user_data_dir"] = isolated_dir

    return Browser(**kwargs), cleanup_dir


def browser_kill_timeout() -> float:
    raw = env_str("BROWSER_USE_KILL_TIMEOUT_SECONDS")
    if not raw:
        return DEFAULT_BROWSER_KILL_TIMEOUT_SECONDS
    try:
        return max(1.0, float(raw))
    except ValueError:
        return DEFAULT_BROWSER_KILL_TIMEOUT_SECONDS


def browser_process_pids_using_path(path: str) -> list[int]:
    if not path:
        return []
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,command="],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return []

    own_pid = os.getpid()
    pids: list[int] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        if pid == own_pid:
            continue
        command = parts[1]
        lower = command.lower()
        if path in command and ("chrom" in lower or "chrome" in lower):
            pids.append(pid)
    return pids


def terminate_browser_processes_using_path(path: str) -> None:
    pids = browser_process_pids_using_path(path)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    if pids:
        time.sleep(1)
    hard_kill = getattr(signal, "SIGKILL", signal.SIGTERM)
    for pid in browser_process_pids_using_path(path):
        try:
            os.kill(pid, hard_kill)
        except (ProcessLookupError, PermissionError):
            pass


async def close_browser(browser: Browser, cleanup_dir: str) -> None:
    try:
        await asyncio.wait_for(browser.kill(), timeout=browser_kill_timeout())
    except Exception:
        pass
    terminate_browser_processes_using_path(cleanup_dir)
    shutil.rmtree(cleanup_dir, ignore_errors=True)


def model_name(prefix: str) -> str:
    return "".join(part.capitalize() for part in prefix.replace("-", "_").split("_") if part) or "Output"


def enum_type(name: str, values: list[Any]) -> type[Any]:
    if all(isinstance(v, str) for v in values):
        return Literal.__getitem__(tuple(values))  # type: ignore[attr-defined]
    members = {f"V{i}": value for i, value in enumerate(values)}
    return Enum(name, members)


def schema_type(name: str, schema: dict[str, Any]) -> tuple[type[Any], bool]:
    """Convert the small JSON Schema subset emitted by our Zod schemas."""

    if "$ref" in schema:
        raise ValueError("$ref schemas are not supported by the local bridge")

    for union_key in ("anyOf", "oneOf"):
        if union_key in schema:
            variants = schema[union_key]
            non_null = [s for s in variants if s.get("type") != "null"]
            nullable = len(non_null) != len(variants)
            if len(non_null) == 1:
                inner, _ = schema_type(name, non_null[0])
                return (inner | type(None), True) if nullable else (inner, False)
            converted = [schema_type(f"{name}Option{i}", s)[0] for i, s in enumerate(non_null)]
            if nullable:
                converted.append(type(None))
            return Union.__getitem__(tuple(converted)), nullable  # type: ignore[attr-defined]

    if "enum" in schema:
        typ = enum_type(name, schema["enum"])
        return typ, False

    typ = schema.get("type")
    if isinstance(typ, list):
        nullable = "null" in typ
        non_null_types = [t for t in typ if t != "null"]
        inner, _ = schema_type(name, {**schema, "type": non_null_types[0] if non_null_types else "string"})
        return (inner | type(None), True) if nullable else (inner, False)

    if typ == "object" or "properties" in schema:
        required = set(schema.get("required") or [])
        fields: dict[str, tuple[type[Any], Any]] = {}
        for prop, prop_schema in (schema.get("properties") or {}).items():
            prop_type, nullable = schema_type(f"{name}{model_name(prop)}", prop_schema)
            default = None if nullable or prop not in required else ...
            fields[prop] = (prop_type, Field(default=default))
        return create_model(name, __base__=BaseModel, **fields), False

    if typ == "array":
        item_type, _ = schema_type(f"{name}Item", schema.get("items") or {"type": "string"})
        return list[item_type], False
    if typ == "integer":
        return int, False
    if typ == "number":
        return float, False
    if typ == "boolean":
        return bool, False
    if typ == "null":
        return type(None), True
    return str, False


def output_model(schema: dict[str, Any] | None) -> type[BaseModel] | None:
    if not schema:
        return None
    typ, _ = schema_type("BrowserUseOutput", schema)
    if not isinstance(typ, type) or not issubclass(typ, BaseModel):
        raise ValueError("structured output schema must be a JSON object schema")
    return typ


def empty_array_object_output(schema: dict[str, Any] | None) -> dict[str, list[Any]] | None:
    """Return a safe empty object for schemas shaped like {items: [...]}."""

    if not schema or schema.get("type") != "object":
        return None
    required = schema.get("required") or []
    properties = schema.get("properties") or {}
    if not required:
        return None

    out: dict[str, list[Any]] = {}
    for prop in required:
        prop_schema = properties.get(prop) or {}
        if prop_schema.get("type") != "array":
            return None
        out[prop] = []
    return out


def strip_json_fence(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 2 and lines[-1].strip() == "```":
            text = "\n".join(lines[1:-1]).strip()
    return text


def parse_structured_json(raw: str) -> Any:
    text = strip_json_fence(raw)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        first_obj = text.find("{")
        first_arr = text.find("[")
        starts = [i for i in (first_obj, first_arr) if i >= 0]
        if not starts:
            raise
        start = min(starts)
        end = max(text.rfind("}"), text.rfind("]"))
        if end <= start:
            raise
        return json.loads(text[start : end + 1])


def parse_final_result(raw: str | None, schema: dict[str, Any] | None) -> Any:
    if raw is None:
        empty = empty_array_object_output(schema)
        if empty is not None:
            return empty
        return None
    if not schema:
        return raw

    text = strip_json_fence(raw)
    if text == "":
        empty = empty_array_object_output(schema)
        if empty is not None:
            return empty
        return None
    return parse_structured_json(text)


async def run_task(browser: Browser, request: dict[str, Any]) -> None:
    request_id = request.get("request_id")
    try:
        schema = request.get("schema")
        model_cls = output_model(schema)
        agent = Agent(
            task=request["prompt"],
            llm=make_llm(request.get("model")),
            browser=browser,
            output_model_schema=model_cls,
        )
        emit({"type": "progress", "request_id": request_id, "message": "local agent started"})
        history = await agent.run()
        emit(
            {
                "type": "done",
                "request_id": request_id,
                "output": parse_final_result(history.final_result(), schema),
            }
        )
    except Exception as exc:
        emit({"type": "error", "request_id": request_id, "message": str(exc)})


async def main() -> None:
    browser, cleanup_dir = make_browser()
    try:
        await browser.start()
    except Exception as exc:
        emit({"type": "error", "message": str(exc)})
        shutil.rmtree(cleanup_dir, ignore_errors=True)
        raise SystemExit(1)

    try:
        emit({"type": "ready", "session_id": os.getenv("SCHOOLYANK_BROWSER_SESSION_ID", "local")})

        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            try:
                request = json.loads(line)
            except json.JSONDecodeError as exc:
                emit({"type": "error", "message": f"invalid request json: {exc}"})
                continue

            if request.get("type") == "stop":
                break
            if request.get("type") != "run":
                emit({"type": "error", "message": f"unknown request type: {request.get('type')}"})
                continue
            await run_task(browser, request)
    finally:
        await close_browser(browser, cleanup_dir)


if __name__ == "__main__":
    asyncio.run(main())
