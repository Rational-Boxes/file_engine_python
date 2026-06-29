"""
Typed exceptions for the FileEngine client.

Failures surface as informative exceptions instead of falsy returns. Every
exception derives from :class:`FileEngineError`, which carries structured
context — the high-level ``operation``, the target ``uid``, the gRPC
``status_code`` (when the failure was a transport-level error), and the
server-supplied ``server_error`` message. The :attr:`FileEngineError.transient`
flag tells a caller whether retrying is reasonable: it is ``True`` for the
availability errors raised during a primary-database failover (the service is
temporarily read-only) or when the server is unreachable.

Hierarchy::

    FileEngineError                     (base; alias: FileSystemError)
    ├── ServerUnreachableError          transient — transport failure / timeout
    ├── ServiceUnavailableError         transient — server up but cannot serve
    │   └── WriteUnavailableError       transient — write rejected: read-only failover
    ├── AuthenticationError             identity could not be authenticated
    ├── PermissionDeniedError           authenticated but not authorized
    ├── NotFoundError                   target entity / version / key is absent
    ├── AlreadyExistsError              target already exists
    ├── InvalidRequestError             malformed / rejected request
    └── OperationError                  generic server-reported failure
"""

__all__ = [
    "FileEngineError", "FileSystemError",
    "ServerUnreachableError", "ServiceUnavailableError", "WriteUnavailableError",
    "AuthenticationError", "PermissionDeniedError",
    "NotFoundError", "AlreadyExistsError", "InvalidRequestError", "OperationError",
]


class FileEngineError(Exception):
    """Base class for every FileEngine client error.

    Attributes:
        message: Human-readable description.
        operation: The client method that failed (e.g. ``"put"``).
        uid: The target entity UID, when applicable.
        status_code: The gRPC ``StatusCode`` for transport-level failures, else None.
        server_error: The raw error string the server returned, when present.
        transient: True if the condition is expected to clear and a retry is
            reasonable (failover read-only window, server unreachable).
    """

    #: Default transient-ness for the class; instances may override.
    transient = False

    def __init__(self, message, *, operation=None, uid=None, status_code=None,
                 server_error=None, transient=None):
        self.message = message
        self.operation = operation
        self.uid = uid
        self.status_code = status_code
        self.server_error = server_error
        if transient is not None:
            self.transient = transient
        super().__init__(self._format())

    def _format(self):
        ctx = []
        if self.operation:
            ctx.append(f"op={self.operation}")
        if self.uid:
            ctx.append(f"uid={self.uid}")
        if self.status_code is not None:
            ctx.append(f"grpc={getattr(self.status_code, 'name', self.status_code)}")
        if self.transient:
            ctx.append("transient")
        return f"{self.message} ({', '.join(ctx)})" if ctx else str(self.message)


# --- availability (transient — safe to retry) ------------------------------ #
class ServerUnreachableError(FileEngineError):
    """The server could not be reached (connection refused, timeout, etc.)."""
    transient = True


class ServiceUnavailableError(FileEngineError):
    """The server is reachable but cannot currently serve the request."""
    transient = True


class WriteUnavailableError(ServiceUnavailableError):
    """A write/mutating operation was rejected because the service is
    temporarily read-only — the primary database is disconnected and the
    server has failed over to a read-only replica. Writes resume once the
    primary is reachable again, so the caller may retry later."""
    transient = True


# --- authentication / authorization ---------------------------------------- #
class AuthenticationError(FileEngineError):
    """The request identity could not be authenticated."""


class PermissionDeniedError(FileEngineError):
    """The identity is authenticated but lacks permission for the operation."""


# --- request / state ------------------------------------------------------- #
class NotFoundError(FileEngineError):
    """The target entity, version, or metadata key does not exist."""


class AlreadyExistsError(FileEngineError):
    """The target already exists."""


class InvalidRequestError(FileEngineError):
    """The request was malformed or otherwise rejected as invalid."""


class OperationError(FileEngineError):
    """The server reported a failure that no more specific class matched."""


# Backwards-compatible alias for the original public exception name.
FileSystemError = FileEngineError
