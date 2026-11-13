import pytest
import requests

from fakes import FakeResponse, FakeSession
from rearc_ingest.config import ConfigError, load_user_agent
from rearc_ingest.http import FetchError, Fetcher, ForbiddenError, TransientError

URL = "https://example.invalid/file"


def fetcher_for(responses, *, sleeps=None, max_attempts=4):
    queue = list(responses)

    def handler(url):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    session = FakeSession(handler)
    record = sleeps if sleeps is not None else []
    fetcher = Fetcher("UA (x@example.com)", session=session, sleep=record.append,
                      max_attempts=max_attempts, backoff_base=2.0)
    return fetcher, session


def test_403_is_tried_exactly_once_and_raises():
    fetcher, session = fetcher_for([FakeResponse(403), FakeResponse(200, b"never")])
    with pytest.raises(ForbiddenError, match="403"):
        fetcher.get(URL)
    assert len(session.calls) == 1


def test_5xx_is_retried_with_exponential_backoff():
    sleeps = []
    fetcher, session = fetcher_for(
        [FakeResponse(503), FakeResponse(502), FakeResponse(200, b"ok")], sleeps=sleeps)
    assert fetcher.get(URL) == b"ok"
    assert len(session.calls) == 3
    assert sleeps == [2.0, 4.0]


@pytest.mark.parametrize("error", [
    requests.Timeout("slow"),
    requests.ConnectionError("reset"),
    requests.exceptions.ChunkedEncodingError("broken"),
])
def test_timeouts_and_connection_errors_are_retried(error):
    fetcher, session = fetcher_for([error, FakeResponse(200, b"ok")])
    assert fetcher.get(URL) == b"ok"
    assert len(session.calls) == 2


def test_other_4xx_is_not_retried():
    fetcher, session = fetcher_for([FakeResponse(404), FakeResponse(200, b"ok")])
    with pytest.raises(FetchError, match="404") as caught:
        fetcher.get(URL)
    assert not isinstance(caught.value, TransientError)
    assert len(session.calls) == 1


def test_gives_up_after_max_attempts():
    fetcher, session = fetcher_for([FakeResponse(500)] * 3, max_attempts=3)
    with pytest.raises(TransientError, match="gave up after 3 attempts"):
        fetcher.get(URL)
    assert len(session.calls) == 3


def test_failed_validation_is_retried():
    fetcher, session = fetcher_for([FakeResponse(200, b"shor"), FakeResponse(200, b"short")])
    body = fetcher.get(URL, validate=lambda c: None if len(c) == 5 else "truncated")
    assert body == b"short"
    assert len(session.calls) == 2


def test_user_agent_header_is_sent():
    fetcher, session = fetcher_for([FakeResponse(200, b"ok")])
    fetcher.get(URL)
    assert session.calls[0][1]["User-Agent"] == "UA (x@example.com)"


def test_requests_are_paced():
    sleeps, now = [], [0.0]
    session = FakeSession(lambda url: FakeResponse(200, b"ok"))
    fetcher = Fetcher("UA (x@example.com)", session=session, pause=1.0,
                      sleep=lambda s: (sleeps.append(s), now.__setitem__(0, now[0] + s)),
                      clock=lambda: now[0])
    fetcher.get(URL)
    now[0] += 0.25  # 0.25s pass before the next request
    fetcher.get(URL)
    assert sleeps == [0.75]


@pytest.mark.parametrize("environ, message", [
    ({}, "not set"),
    ({"BLS_USER_AGENT": "   "}, "not set"),
    ({"BLS_USER_AGENT": "Just A Name"}, "contact info"),
])
def test_user_agent_without_contact_info_is_refused(environ, message):
    with pytest.raises(ConfigError, match=message):
        load_user_agent(environ)
