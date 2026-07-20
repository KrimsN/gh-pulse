"""Тесты чтения хвоста лог-файлов (задача 4.4) — обычная файловая система, `tmp_path`, без датастора."""

from pathlib import Path

from app.admin.logs_viewer import read_log_tail


def test_read_log_tail_returns_empty_list_when_file_missing(tmp_path: Path) -> None:
    assert read_log_tail(tmp_path, "gh-collector", lines=100) == []


def test_read_log_tail_returns_last_n_lines_in_order(tmp_path: Path) -> None:
    (tmp_path / "pulse-api.log").write_text("\n".join(f"line-{i}" for i in range(10)) + "\n", encoding="utf-8")

    tail = read_log_tail(tmp_path, "pulse-api", lines=3)

    assert tail == ["line-7", "line-8", "line-9"]


def test_read_log_tail_returns_all_lines_when_fewer_than_requested(tmp_path: Path) -> None:
    (tmp_path / "pulse-consumer.log").write_text("only-one-line\n", encoding="utf-8")

    tail = read_log_tail(tmp_path, "pulse-consumer", lines=100)

    assert tail == ["only-one-line"]


def test_read_log_tail_filters_by_level_case_insensitively(tmp_path: Path) -> None:
    lines = [
        '{"level": "info", "event": "started"}',
        '{"level": "error", "event": "crashed"}',
        '{"level": "info", "event": "stopped"}',
    ]
    (tmp_path / "pulse-api.log").write_text("\n".join(lines) + "\n", encoding="utf-8")

    tail = read_log_tail(tmp_path, "pulse-api", lines=100, level="error")

    assert tail == ['{"level": "error", "event": "crashed"}']


def test_read_log_tail_level_filter_applies_before_truncating_to_lines(tmp_path: Path) -> None:
    lines = ['{"level": "error", "event": "first"}', '{"level": "info", "event": "noise"}'] * 5
    (tmp_path / "pulse-api.log").write_text("\n".join(lines) + "\n", encoding="utf-8")

    tail = read_log_tail(tmp_path, "pulse-api", lines=2, level="error")

    assert len(tail) == 2
    assert all("error" in line for line in tail)
