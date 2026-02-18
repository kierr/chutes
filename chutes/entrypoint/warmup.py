"""
Warm up a chute.
"""

import os
import asyncio
import aiohttp
import orjson as json
import sys
import time
from loguru import logger
import typer
from chutes.config import get_config
from chutes.entrypoint._shared import load_chute
from chutes.util.auth import sign_request


STREAM_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=None)
POLL_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)


def _new_signed_headers(purpose: str) -> dict:
    headers, _ = sign_request(purpose=purpose)
    return headers


async def _iter_sse_data(content):
    """
    Yield raw SSE `data:` payload bytes, handling chunk fragmentation.
    """
    buffer = b""
    async for chunk in content.iter_any():
        if not chunk:
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            line = line.rstrip(b"\r")
            if not line or line.startswith(b":") or not line.startswith(b"data:"):
                continue
            payload = line[5:].lstrip()
            if payload:
                yield payload
    if buffer:
        line = buffer.rstrip(b"\r")
        if line and not line.startswith(b":") and line.startswith(b"data:"):
            payload = line[5:].lstrip()
            if payload:
                yield payload


async def poll_for_instances(chute_name: str, config, poll_interval: float = 2.0, max_wait: float = 600.0):
    """
    Poll for instances of a chute. Returns a list of instance info dicts (with instance_id and status).
    
    Args:
        chute_name: Name or ID of the chute
        config: Config object
        poll_interval: Seconds between polls
        max_wait: Maximum seconds to wait for an instance (default 10 minutes)
    
    Returns:
        List of dicts with 'instance_id' and status information
    """
    start_time = time.monotonic()
    async with aiohttp.ClientSession(
        base_url=config.generic.api_base_url,
        timeout=POLL_TIMEOUT,
    ) as session:
        while time.monotonic() - start_time < max_wait:
            headers = _new_signed_headers("chutes")
            try:
                async with session.get(
                    f"/chutes/{chute_name}",
                    headers=headers,
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        instances = data.get("instances", [])
                        if instances:
                            # Return all instances with their status info
                            instance_infos = [
                                {
                                    "instance_id": inst.get("instance_id"),
                                    "active": inst.get("active", False),
                                    "verified": inst.get("verified", False),
                                    "region": inst.get("region", "n/a"),
                                    "last_verified_at": inst.get("last_verified_at"),
                                }
                                for inst in instances
                                if inst.get("instance_id")
                            ]
                            if instance_infos:
                                return instance_infos
                        await asyncio.sleep(poll_interval)
                        continue

                    error_text = await response.text()
                    if response.status == 404:
                        raise ValueError(f"Chute '{chute_name}' not found: {error_text}")
                    if response.status in (401, 403):
                        raise PermissionError(
                            f"Access denied while polling chute '{chute_name}' "
                            f"[status={response.status}]: {error_text}"
                        )
                    if 400 <= response.status < 500:
                        raise RuntimeError(
                            f"Polling request rejected for chute '{chute_name}' "
                            f"[status={response.status}]: {error_text}"
                        )
                    logger.debug(
                        f"Failed to get chute '{chute_name}' "
                        f"(status {response.status}): {error_text}"
                    )
            except (ValueError, PermissionError, RuntimeError):
                raise
            except Exception as e:
                logger.debug(f"Error polling for instances: {e}")

            await asyncio.sleep(poll_interval)

        # Timeout reached
        raise TimeoutError(f"No instances found for chute {chute_name} within {max_wait} seconds")


async def stream_instance_logs(instance_id: str, config, backfill: int = 100):
    """
    Stream logs from an instance.
    
    Raises:
        aiohttp.ClientResponseError: If the request fails
        RuntimeError: For streaming errors
    """
    # Sign the request with purpose for instances endpoint
    headers = _new_signed_headers("logs")
    async with aiohttp.ClientSession(
        base_url=config.generic.api_base_url,
        timeout=STREAM_TIMEOUT,
    ) as session:
        async with session.get(
            f"/instances/{instance_id}/logs",
            headers=headers,
            params={"backfill": str(backfill)},
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Failed to stream logs from instance {instance_id}: {error_text}",
                )

            logger.info(f"Streaming logs from instance {instance_id}...")
            try:
                async for payload in _iter_sse_data(response.content):
                    try:
                        data = json.loads(payload)
                    except json.JSONDecodeError as e:
                        logger.debug(f"Skipping unparseable SSE payload: {payload[:100]}, error: {e}")
                        continue

                    if data.get("error"):
                        raise RuntimeError(
                            f"Log stream returned an error for instance {instance_id}: {data['error']}"
                        )

                    log_message = data.get("log")
                    if log_message is None:
                        continue
                    if not isinstance(log_message, str):
                        log_message = str(log_message)
                    if log_message == "":
                        continue

                    sys.stdout.buffer.write(log_message.encode("utf-8", errors="replace") + b"\n")
                    sys.stdout.buffer.flush()
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, OSError) as e:
                # Connection errors - instance might have been deleted
                raise RuntimeError(
                    f"Stream ended for instance {instance_id} "
                    f"(instance may have been deleted): {e}"
                ) from e


async def monitor_warmup(chute_name: str, config, headers):
    """
    Monitor the warmup stream and log status updates.
    Raises if the stream fails before the chute reaches `hot`.
    """
    hot = False
    async with aiohttp.ClientSession(
        base_url=config.generic.api_base_url,
        timeout=STREAM_TIMEOUT,
    ) as session:
        async with session.get(
            f"/chutes/warmup/{chute_name}",
            headers=headers,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Failed warmup monitoring for chute '{chute_name}': {error_text}",
                )

            async for payload in _iter_sse_data(response.content):
                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError as e:
                    logger.debug(f"Skipping unparseable warmup payload: {payload[:100]}, error: {e}")
                    continue

                status = chunk.get("status")
                message = chunk.get("log", "")
                if status == "hot":
                    logger.success(message or f"Chute '{chute_name}' is hot")
                    hot = True
                    break
                if status is not None or message:
                    logger.warning(f"Status: {status or 'unknown'} -- {message}")

    if not hot:
        raise RuntimeError(
            f"Warmup stream ended before chute '{chute_name}' reached hot status"
        )


async def poll_and_stream_logs(chute_name: str, config):
    """
    Poll for instances and stream logs when found. Tries multiple instances if one fails.
    """
    instance_infos = await poll_for_instances(chute_name, config)
    logger.info(f"Found {len(instance_infos)} instance(s), attempting to stream logs...")

    # Try each instance until one works
    # Keep trying instances even if they get deleted during streaming
    tried_instance_ids = set()
    max_retries = 10  # Limit retries to avoid infinite loops

    for attempt in range(max_retries):
        # Refresh instance list in case instances were deleted
        try:
            current_instance_infos = await poll_for_instances(
                chute_name,
                config,
                poll_interval=1.0,
                max_wait=5.0,
            )
            # Filter out instances we've already tried
            available_instances = [
                inst for inst in current_instance_infos if inst["instance_id"] not in tried_instance_ids
            ]

            if not available_instances:
                # No new instances available
                if tried_instance_ids:
                    if attempt < max_retries - 1:
                        logger.info("All instances have been tried or deleted. Waiting for new instances...")
                        await asyncio.sleep(2.0)
                        continue
                    raise RuntimeError("No new instances available after retries")
                raise RuntimeError("No instances available for log streaming")

            # Try the first available instance
            inst_info = available_instances[0]
            instance_id = inst_info["instance_id"]
            tried_instance_ids.add(instance_id)

            logger.info(f"Attempting to stream logs from instance {instance_id}...")

            try:
                # Stream logs - this will continue until interrupted or stream ends
                await stream_instance_logs(instance_id, config)
                # If we get here, streaming completed successfully (unlikely for a stream)
                return
            except asyncio.CancelledError:
                # User interrupted, don't try other instances
                raise
            except Exception as e:
                # Stream ended (instance deleted, connection error, etc.)
                logger.warning(f"Stream ended for instance {instance_id}: {e}")
                logger.info("Looking for another instance to stream from...")
                # Continue the loop to try another instance
                continue
        except TimeoutError:
            # No instances found when refreshing
            if tried_instance_ids and attempt < max_retries - 1:
                logger.info("No new instances found. Waiting...")
                await asyncio.sleep(2.0)
                continue
            raise

    # Exhausted retries
    raise RuntimeError(f"Failed to maintain stream after {max_retries} attempts")


def warmup_chute(
    chute_id_or_ref_str: str = typer.Argument(
        ...,
        help="The chute file to warm up, format filename:chutevarname",
    ),
    config_path: str = typer.Option(
        None, help="Custom path to the chutes config (credentials, API URL, etc.)"
    ),
    debug: bool = typer.Option(False, help="enable debug logging"),
    stream_logs: bool = typer.Option(
        False,
        help="stream logs while warmup is in progress",
    ),
):
    async def warmup():
        """
        Do the warmup.
        """
        nonlocal chute_id_or_ref_str, config_path, debug, stream_logs
        chute_name = chute_id_or_ref_str
        if ":" in chute_id_or_ref_str and os.path.exists(chute_id_or_ref_str.split(":")[0] + ".py"):
            from chutes.chute.base import Chute

            _, chute = load_chute(chute_id_or_ref_str, config_path=config_path, debug=debug)
            chute_name = chute.name if isinstance(chute, Chute) else chute.chute.name
        config = get_config()
        if config_path:
            os.environ["CHUTES_CONFIG_PATH"] = config_path
        headers = _new_signed_headers("chutes")

        if stream_logs:
            # Run warmup monitoring and log streaming in parallel
            warmup_task = asyncio.create_task(monitor_warmup(chute_name, config, headers))
            poll_task = asyncio.create_task(poll_and_stream_logs(chute_name, config))

            # Warmup completion determines command success; stop log streaming once warmup is done.
            try:
                await warmup_task
            finally:
                if not poll_task.done():
                    poll_task.cancel()
                try:
                    poll_results = await asyncio.gather(poll_task, return_exceptions=True)
                    poll_error = poll_results[0] if poll_results else None
                    if poll_error and not isinstance(poll_error, asyncio.CancelledError):
                        logger.warning(f"Log streaming ended with error: {poll_error}")
                except Exception:
                    pass
        else:
            # Just monitor warmup
            await monitor_warmup(chute_name, config, headers)

    return asyncio.run(warmup())
