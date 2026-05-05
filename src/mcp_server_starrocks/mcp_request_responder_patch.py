# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Work around MCP SDK assertion when client cancels a request while a sync tool still finishes.

If the client sends ``notifications/cancelled``, ``RequestResponder.cancel()`` marks the
request completed and sends an error. Sync tools run in a thread pool and keep running;
when they return, ``Server._handle_request`` still calls ``respond()``, which raises
``AssertionError: Request already responded to`` and tears down the session.

Upstream pattern (compare FastMCP initialize error handling): skip the second send when
the responder is already completed.
"""

from __future__ import annotations

from typing import Any

from loguru import logger
from mcp.shared.session import RequestResponder

_orig_respond = RequestResponder.respond
_patch_applied = False


async def _respond_skip_if_already_completed(self: RequestResponder[Any, Any], response: Any) -> None:
    if self._completed:  # type: ignore[attr-defined]
        logger.debug(
            "Skipping MCP respond for already-completed request_id={} (e.g. client cancelled while sync tool finished)",
            self.request_id,
        )
        return
    return await _orig_respond(self, response)


def apply_mcp_request_responder_duplicate_guard() -> None:
    """Idempotent: patch ``RequestResponder.respond`` once per process."""
    global _patch_applied
    if _patch_applied:
        return
    RequestResponder.respond = _respond_skip_if_already_completed  # type: ignore[method-assign]
    _patch_applied = True
