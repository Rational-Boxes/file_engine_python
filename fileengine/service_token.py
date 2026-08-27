# Copyright (C) 2026 James Hickman
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Carry the calling service's credential on every RPC.

See ``file_engine_core/design_documents/PROPOSAL_service_authentication.md``.

The core cannot otherwise tell which internal service is calling it: every RPC
arrives with an end-user identity and nothing about its origin. The token both
proves the caller is legitimate and names it, so ``source_iface`` finally records
which door an action came through, and the core can refuse a service the
operations it has no business performing.

**This SDK ships no token of its own.** §8.2 of the proposal declines to issue
the SDKs an identity: they speak gRPC directly and are documented as safe only
server-side, so minting them a credential would legitimise the weakest path.
Instead the SDK *carries* whatever credential the service embedding it was
issued. A server-side user who wants core access obtains a service identity
deliberately, which is the point.

Attached by a channel interceptor rather than at each call site. The SDK makes
~40 stub calls; adding metadata to each would eventually miss one, and the miss
would surface as an UNAUTHENTICATED in production rather than as an error at
build time. This mirrors the core, which validates in one server interceptor for
exactly the same reason.
"""
from __future__ import annotations

import os
from typing import Optional

import grpc

# A dedicated header, not ``authorization: Bearer``. The bridges already carry
# end-user bearer tokens under that name, and two different credentials sharing
# a header is how the wrong one gets validated — or logged by a redaction rule
# written for the other.
SERVICE_TOKEN_METADATA_KEY = "x-fe-service-token"


def load_service_token() -> Optional[str]:
    """The token this process should present, or None.

    ``FILEENGINE_SERVICE_TOKEN_FILE`` wins over the plain variable. That is the
    container path: an init writes the credential into a shared volume before
    the service starts, which is what lets a token exist that did not exist at
    ``compose up`` time — and it keeps the secret out of container metadata,
    where an environment variable is visible to ``docker inspect``.
    """
    path = os.environ.get("FILEENGINE_SERVICE_TOKEN_FILE", "").strip()
    if path:
        try:
            with open(path) as handle:
                # Trimmed: a file written by a shell almost always ends in a
                # newline, and a token with a trailing newline authenticates as
                # nobody, with nothing in the error to say why.
                token = handle.readline().strip()
            if token:
                return token
        except OSError:
            # Fall through to the environment. Failing hard here would make a
            # missing file at startup indistinguishable from a bad credential
            # later, and the env var is a legitimate configuration.
            pass

    token = os.environ.get("FILEENGINE_SERVICE_TOKEN", "").strip()
    return token or None


class _ServiceTokenInterceptor(
        grpc.UnaryUnaryClientInterceptor,
        grpc.UnaryStreamClientInterceptor,
        grpc.StreamUnaryClientInterceptor,
        grpc.StreamStreamClientInterceptor):
    """Adds the token to every call, of every arity.

    All four arities are implemented deliberately: the streaming RPCs
    (``StreamFileUpload``, ``StreamFileDownload``) are how file content moves,
    so covering only the unary ones would leave the highest-volume and most
    security-relevant paths unauthenticated — and it would look like it worked,
    because everything else would pass.
    """

    def __init__(self, token: str):
        self._token = token

    def _with_token(self, client_call_details):
        metadata = list(client_call_details.metadata or [])
        metadata.append((SERVICE_TOKEN_METADATA_KEY, self._token))
        return client_call_details._replace(metadata=metadata)

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._with_token(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._with_token(details), request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        return continuation(self._with_token(details), request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        return continuation(self._with_token(details), request_iterator)


def authenticated_channel(channel: grpc.Channel, token: Optional[str] = None) -> grpc.Channel:
    """Wrap ``channel`` so every call carries the service token.

    Returns the channel unchanged when there is no token, which is what keeps
    this usable during the migration: a caller that has not been issued a
    credential yet still works against a core running with
    ``FILEENGINE_SERVICE_AUTH_REQUIRED=false``, and its records keep saying
    ``grpc`` — which is how the rollout stays self-tracking.
    """
    token = token if token is not None else load_service_token()
    if not token:
        return channel
    return grpc.intercept_channel(channel, _ServiceTokenInterceptor(token))
