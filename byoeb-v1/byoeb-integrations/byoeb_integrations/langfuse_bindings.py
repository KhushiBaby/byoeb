from __future__ import annotations

import contextlib
from contextvars import ContextVar
import mimetypes
from typing import Any, Generator

import httpx
from wrapt import wrap_function_wrapper

from langfuse._client.get_client import get_client
from langfuse.api import MediaContentType
from langfuse.media import LangfuseMedia

_AUDIO_CALL: ContextVar[dict[str, Any] | None] = ContextVar("langfuse_audio_call", default=None)
_OBS_KEY = "langfuse_audio_observation"
_HOOKED_KEY = "_langfuse_audio_hooks_registered"


def _media(file: Any) -> LangfuseMedia | None:
    if file is None:
        return None

    if isinstance(file, (bytes, bytearray)):
        data, name = bytes(file), "audio.wav"
    else:
        pos = file.tell() if hasattr(file, "tell") else None
        data = file.read()
        if pos is not None and hasattr(file, "seek"):
            file.seek(pos)
        name = getattr(file, "name", "audio.wav")

    guessed, _ = mimetypes.guess_type(name)
    try:
        mime = MediaContentType(guessed)
    except (ValueError, TypeError):
        mime = MediaContentType.AUDIO_WAV
    return LangfuseMedia(content_bytes=data, content_type=mime)


def _usage_details(usage: Any) -> dict[str, Any] | None:
    if usage is None:
        return None

    data = usage if isinstance(usage, dict) else usage.__dict__

    if data.get("type") == "duration":
        return {"input": data["seconds"], "unit": "seconds"}

    if data.get("type") == "tokens":
        result = {
            "input": data["input_tokens"],
            "output": data["output_tokens"],
            "total": data["total_tokens"],
        }
        token_details = data.get("input_token_details")
        if token_details is not None:
            nested = token_details if isinstance(token_details, dict) else token_details.__dict__
            result["input_token_details"] = {k: v for k, v in nested.items() if v is not None}
        return result

    return {k: v for k, v in data.items() if v is not None}


def _sync_request_hook(request: httpx.Request) -> None:
    path = request.url.path
    if "/audio/transcriptions" not in path and "/audio/translations" not in path:
        return

    if _OBS_KEY in request.extensions:
        return

    call = _AUDIO_CALL.get()
    if call is None:
        return

    request.extensions[_OBS_KEY] = get_client().start_observation(
        as_type="generation",
        name="OpenAI-generation",
        model=call["model"],
        model_parameters=call["model_parameters"],
        input=call["input"],
        metadata={
            "provider": call["provider"],
            "endpoint": "translations" if "/audio/translations" in path else "transcriptions",
            "attempt": int(request.headers.get("x-stainless-retry-count", "0")) + 1,
            "path": path,
            "method": request.method,
        },
    )

async def _async_request_hook(request: httpx.Request) -> None:
    _sync_request_hook(request)


def _process_response_hook(response: httpx.Response) -> None:
    if _OBS_KEY not in response.request.extensions:
        return

    generation = response.request.extensions[_OBS_KEY]
    payload = response.json()
    usage = _usage_details(payload.get("usage"))

    update: dict[str, Any] = {
        "output": payload.get("text", payload),
        "metadata": {
            "status_code": response.status_code,
            "request_id": response.headers.get("x-request-id"),
            "attempt": int(response.request.headers.get("x-stainless-retry-count", "0")) + 1,
        },
    }

    if usage is not None:
        update["usage_details"] = usage
        update["usage"] = usage

        cost = payload["usage"].get("cost") if isinstance(payload["usage"], dict) else getattr(payload["usage"], "cost", None)
        if cost is not None:
            update["cost_details"] = {"total": cost}

    if response.status_code >= 400:
        error = payload.get("error", payload)
        update["level"] = "ERROR"
        update["status_message"] = (
            error.get("message", f"HTTP {response.status_code}")
            if isinstance(error, dict)
            else f"HTTP {response.status_code}"
        )

    generation.update(**update).end()


async def _async_response_hook(response: httpx.Response) -> None:
    path = response.request.url.path
    if "/audio/transcriptions" not in path and "/audio/translations" not in path:
        return

    await response.aread()
    _process_response_hook(response)


def _sync_response_hook(response: httpx.Response) -> None:
    path = response.request.url.path
    if "/audio/transcriptions" not in path and "/audio/translations" not in path:
        return

    response.read()
    _process_response_hook(response)


def _post_init(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
    result = wrapped(*args, **kwargs)

    client = instance._client
    if getattr(client, _HOOKED_KEY, False):
        return result

    if isinstance(client, httpx.AsyncClient):
        client.event_hooks["request"] = [*client.event_hooks.get("request", []), _async_request_hook]
        client.event_hooks["response"] = [*client.event_hooks.get("response", []), _async_response_hook]
    else:
        client.event_hooks["request"] = [*client.event_hooks.get("request", []), _sync_request_hook]
        client.event_hooks["response"] = [*client.event_hooks.get("response", []), _sync_response_hook]

    setattr(client, _HOOKED_KEY, True)
    return result


@contextlib.contextmanager
def _audio_call_context(instance: Any, kwargs: Any) -> Generator[None, None, None]:
    model_parameters = {
        k: v
        for k, v in {
            "language": kwargs.get("language"),
            "temperature": kwargs.get("temperature"),
            "response_format": kwargs.get("response_format"),
            "prompt": kwargs.get("prompt"),
            "include": kwargs.get("include"),
            "timestamp_granularities": kwargs.get("timestamp_granularities"),
            "chunking_strategy": kwargs.get("chunking_strategy"),
        }.items()
        if v is not None
    }

    token = _AUDIO_CALL.set(
        {
            "provider": "azure" if "azure" in str(instance._client.base_url) else "openai",
            "model": kwargs.get("model"),
            "model_parameters": model_parameters or None,
            "input": {"audio": _media(kwargs.get("file")), **model_parameters},
        }
    )
    try:
        yield
    finally:
        _AUDIO_CALL.reset(token)


async def _async_audio_create(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
    with _audio_call_context(instance, kwargs):
        return await wrapped(*args, **kwargs)


def _sync_audio_create(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
    with _audio_call_context(instance, kwargs):
        return wrapped(*args, **kwargs)


def register() -> None:
    wrap_function_wrapper("openai._client", "OpenAI.__init__", _post_init)
    wrap_function_wrapper("openai._client", "AsyncOpenAI.__init__", _post_init)
    wrap_function_wrapper("openai.lib.azure", "AzureOpenAI.__init__", _post_init)
    wrap_function_wrapper("openai.lib.azure", "AsyncAzureOpenAI.__init__", _post_init)

    wrap_function_wrapper("openai.resources.audio.transcriptions", "Transcriptions.create", _sync_audio_create)
    wrap_function_wrapper("openai.resources.audio.transcriptions", "AsyncTranscriptions.create", _async_audio_create)
    wrap_function_wrapper("openai.resources.audio.translations", "Translations.create", _sync_audio_create)
    wrap_function_wrapper("openai.resources.audio.translations", "AsyncTranslations.create", _async_audio_create)