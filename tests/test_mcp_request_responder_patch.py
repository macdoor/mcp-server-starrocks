"""Guard against duplicate MCP responds after client cancellation (sync tools finishing late)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from mcp import types
from mcp.shared.session import RequestResponder

from src.mcp_server_starrocks.mcp_request_responder_patch import (
    apply_mcp_request_responder_duplicate_guard,
)


async def _duplicate_respond_scenario() -> None:
    apply_mcp_request_responder_duplicate_guard()
    session = MagicMock()
    session._send_response = AsyncMock()
    req = types.ClientRequest(types.PingRequest())
    responder = RequestResponder(
        request_id=1,
        request_meta=None,
        request=req,
        session=session,
        on_complete=lambda _r: None,
    )
    result = types.ServerResult(types.EmptyResult())
    with responder:
        await responder.respond(result)
        await responder.respond(result)
    assert session._send_response.await_count == 1


def test_second_respond_is_noop_when_already_completed():
    asyncio.run(_duplicate_respond_scenario())
