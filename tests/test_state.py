from types import SimpleNamespace

import pytest

from taxi_pipeline import state


def test_second_writer_is_refused(tmp_path):
    with state.run_lock(tmp_path):
        with pytest.raises(RuntimeError, match="run lock"):
            with state.run_lock(tmp_path):
                pass
    with state.run_lock(tmp_path):
        pass


def test_windows_lock_uses_msvcrt(tmp_path, monkeypatch):
    calls = []
    fake = SimpleNamespace(
        LK_NBLCK=2, LK_UNLCK=0,
        locking=lambda fileno, mode, size: calls.append((mode, size)),
    )
    monkeypatch.setattr(state, "fcntl", None)
    monkeypatch.setattr(state, "msvcrt", fake)
    with state.run_lock(tmp_path):
        assert calls == [(fake.LK_NBLCK, 1)]
    assert calls == [(fake.LK_NBLCK, 1), (fake.LK_UNLCK, 1)]


def test_windows_lock_contention_is_reported(tmp_path, monkeypatch):
    def busy(fileno, mode, size):
        raise OSError(36, "Resource deadlock avoided")
    monkeypatch.setattr(state, "fcntl", None)
    monkeypatch.setattr(state, "msvcrt", SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=busy))
    with pytest.raises(RuntimeError, match="run lock"):
        with state.run_lock(tmp_path):
            pass
