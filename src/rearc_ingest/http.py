"""HTTP fetching with the retry policy from CLAUDE.md.

- 403: stop immediately and report. Never retried.
- Timeouts, connection errors, 5xx, and a body that fails validation: retried with
  exponential backoff.
- Any other non-200 status: fails without retry.
- Requests from one Fetcher are spaced at least `pause` seconds apart.
"""
from __future__ import annotations

import time
from collections.abc import Callable

import requests

# Returns None if the body is acceptable, or a description of the problem.
Validator = Callable[[bytes], "str | None"]

_TRANSIENT_EXCEPTIONS = (
    requests.Timeout,
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,  # connection dropped mid-body
)


class FetchError(RuntimeError):
    """A request failed and will not be retried."""


class ForbiddenError(FetchError):
    """HTTP 403. The run must stop; retrying would ignore the server's answer."""


class TransientError(FetchError):
    """A failure worth retrying: timeout, connection error, 5xx, or a bad body."""


class InvalidBodyError(TransientError):
    """HTTP 200, but the body failed validation (wrong size, not the expected JSON)."""


class Fetcher:
    def __init__(
        self,
        user_agent: str,
        *,
        session: requests.Session | None = None,
        timeout: float = 60.0,
        max_attempts: int = 4,
        backoff_base: float = 2.0,
        pause: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._user_agent = user_agent
        self._session = session or requests.Session()
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._pause = pause
        self._sleep = sleep
        self._clock = clock
        self._last_request: float | None = None

    def get(self, url: str, validate: Validator | None = None) -> bytes:
        last_error: TransientError | None = None
        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                # Waits 1x, 2x, 4x ... backoff_base seconds before attempts 2, 3, 4 ...
                self._sleep(self._backoff_base * 2 ** (attempt - 2))
            try:
                return self._attempt(url, validate)
            except TransientError as error:
                last_error = error
        # Keep the subclass, so callers can tell a bad body from a network failure.
        raise type(last_error)(
            f"gave up after {self._max_attempts} attempts: {last_error}"
        ) from last_error

    def _attempt(self, url: str, validate: Validator | None) -> bytes:
        self._wait_for_pause()
        try:
            response = self._session.get(
                url, headers={"User-Agent": self._user_agent}, timeout=self._timeout
            )
        except _TRANSIENT_EXCEPTIONS as error:
            raise TransientError(f"{url}: {type(error).__name__}: {error}") from error

        status = response.status_code
        if status == 403:
            raise ForbiddenError(
                f"{url}: HTTP 403 Forbidden. Stopping without retry. "
                "For BLS, check that BLS_USER_AGENT carries valid contact info."
            )
        if 500 <= status <= 599:
            raise TransientError(f"{url}: HTTP {status}")
        if status != 200:
            raise FetchError(f"{url}: HTTP {status}")

        content = response.content
        if validate is not None:
            problem = validate(content)
            if problem:
                raise InvalidBodyError(f"{url}: {problem}")
        return content

    def _wait_for_pause(self) -> None:
        if self._last_request is not None:
            remaining = self._pause - (self._clock() - self._last_request)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request = self._clock()
