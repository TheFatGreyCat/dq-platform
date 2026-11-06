from datetime import datetime, timedelta, timezone
from decimal import Decimal

import console_worker as w

NOW = datetime(2026, 1, 1, 12, 0, 7, tzinfo=timezone.utc)


def test_last_completed_window_is_epoch_aligned_utc():
    s, e = w.last_completed_window(NOW, 3600)
    assert (s, e) == (datetime(2026, 1, 1, 11, tzinfo=timezone.utc), datetime(2026, 1, 1, 12, tzinfo=timezone.utc))
    s, e = w.last_completed_window(datetime(2026, 1, 1, 12, tzinfo=timezone.utc), 300)  # boundary: previous window
    assert e == datetime(2026, 1, 1, 12, tzinfo=timezone.utc) and e - s == timedelta(seconds=300)


def test_rate_exceeds_is_strict_and_needs_min_reports():
    assert w.rate_exceeds(3, 4, 4, Decimal("0.5"))
    assert not w.rate_exceeds(2, 4, 4, Decimal("0.5"))  # equal is not a breach
    assert not w.rate_exceeds(3, 3, 4, Decimal("0.5"))  # below min_reports
    assert not w.rate_exceeds(0, 0, 1, Decimal("0"))


def _result(transport, attempts):
    return w.delivery_result(transport, "http://x", "AID", {"b": 1}, attempts, NOW)


def test_delivery_success_and_idempotency_key():
    seen = []
    r = _result(lambda u, b, h: seen.append((u, b, h)) or 204, 1)
    assert r["state"] == "DELIVERED" and r["delivered_at"] == NOW and r["last_error_code"] is None
    assert seen == [("http://x", {"b": 1}, {"Idempotency-Key": "AID"})]


def test_delivery_backoff_then_failed_after_five():
    for n, delay in enumerate((30, 60, 120, 240), start=1):
        r = _result(lambda *a: 500, n)
        assert r["state"] == "PENDING" and r["next_attempt_at"] == NOW + timedelta(seconds=delay)
        assert r["last_error_code"] == "WEBHOOK_HTTP_ERROR"
    r = _result(lambda *a: 500, 5)
    assert r["state"] == "FAILED" and r["next_attempt_at"] is None


def test_redirect_is_failure_and_network_error_code():
    assert _result(lambda *a: 302, 1)["last_error_code"] == "WEBHOOK_HTTP_ERROR"

    def boom(*a):
        raise OSError("http://secret-url/token")

    r = _result(boom, 1)
    assert r["last_error_code"] == "WEBHOOK_NETWORK_ERROR" and "secret" not in repr(r)


def test_post_transport_disables_redirects(monkeypatch):
    calls = {}

    class Resp:
        status_code = 200
        def close(self): calls["closed"] = True

    monkeypatch.setattr(w.requests, "post", lambda url, **kw: calls.update(kw) or Resp())
    assert w.post_transport("http://x", {"a": 1}, {"Idempotency-Key": "k"}) == 200
    assert calls["allow_redirects"] is False and calls["headers"] == {"Idempotency-Key": "k"} and calls["closed"]


def test_run_loop_survives_tick_errors_without_leaking(caplog):
    worker = w.ConsoleWorker(store=None)
    n = {"ticks": 0}

    def tick():
        n["ticks"] += 1
        raise RuntimeError("postgres://user:pw@host/db")

    worker.tick = tick
    sleeps = []
    stops = iter([False, False, True])
    worker.run(lambda: next(stops), sleeps.append, interval=3)
    assert n["ticks"] == 2 and sleeps == [3, 3]
    assert "pw" not in caplog.text and "WORKER_TICK_FAILED" in caplog.text
