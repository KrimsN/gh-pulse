#!/usr/bin/env python3
"""Прогнать запрос ClickHouse N раз, отдать медиану wall time и статистику чтения.

Намеренно почти без зависимостей: использует clickhouse-connect, если он есть, иначе вызывает
clickhouse-client. Смысл в цифре, которой можно доверять, а не во фреймворке.

Использование:
    python bench_query.py --runs 5 --sql "SELECT ..."
    python bench_query.py --runs 5 --file bench/trending.sql
    python bench_query.py --runs 5 --file bench/trending.sql --warm  # отбросить первый (холодный) прогон
"""

from __future__ import annotations

import argparse
import pathlib
import statistics
import subprocess
import sys
import time


def run_via_client(sql: str) -> tuple[float, int, int]:
    """Вернуть (elapsed_ms, read_rows, read_bytes), выполнив запрос через clickhouse-client."""
    # --format Null: меряем выполнение, а не сериализацию результата.
    started = time.perf_counter()
    proc = subprocess.run(
        ["clickhouse-client", "--query", sql, "--format", "Null", "--print-profile-events"],
        capture_output=True,
        text=True,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    if proc.returncode != 0:
        sys.exit(f"запрос упал:\n{proc.stderr}")
    read_rows = _grep_profile(proc.stderr, "SelectedRows")
    read_bytes = _grep_profile(proc.stderr, "ReadCompressedBytes")
    return elapsed_ms, read_rows, read_bytes


def _grep_profile(stderr: str, key: str) -> int:
    """Выдрать числовое значение profile event по имени; -1, если не нашли."""
    for line in stderr.splitlines():
        if key in line:
            for tok in line.replace(":", " ").split():
                if tok.isdigit():
                    return int(tok)
    return -1


def main() -> None:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql")
    src.add_argument("--file")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--warm", action="store_true", help="отбросить первый прогон как прогрев кэша")
    args = ap.parse_args()

    sql = args.sql or pathlib.Path(args.file).open(encoding="utf-8").read()

    timings: list[float] = []
    rows = bytes_ = -1
    for i in range(args.runs):
        elapsed_ms, rows, bytes_ = run_via_client(sql)
        tag = "cold" if i == 0 else "warm"
        print(f"  прогон {i + 1}: {elapsed_ms:8.1f} мс  ({tag})")
        timings.append(elapsed_ms)

    # При --warm первый прогон холодный и в медиану не идёт.
    considered = timings[1:] if (args.warm and len(timings) > 1) else timings
    label = "warm" if args.warm else "все"
    print()
    print(f"медиана ({label}): {statistics.median(considered):.1f} мс")
    print(f"min/max:          {min(considered):.1f} / {max(considered):.1f} мс")
    print(f"rows_read:        {rows:,}")
    print(f"bytes_read:       {bytes_:,}")
    print("\nВставьте эти цифры в запись PERFORMANCE.md. Отдавайте медиану, отметьте разброс.")


if __name__ == "__main__":
    main()
