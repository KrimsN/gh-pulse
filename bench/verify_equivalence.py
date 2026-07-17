#!/usr/bin/env python3
"""Проверить, что два запроса ClickHouse возвращают идентичные результаты.

Ускорение, изменившее ответ, — это баг, а не победа. Запускать перед записью любой оптимизации
в PERFORMANCE.md.

Сравнивает независимо от порядка: хэширует отсортированные строки, поэтому разница в ORDER BY на
уровне представления не даст ложного расхождения — а вот настоящая разница в данных даст.

Использование:
    python verify_equivalence.py --a bench/trending_baseline.sql --b bench/trending_mv.sql
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import subprocess
import sys


def fetch_sorted_hash(sql: str) -> tuple[str, int]:
    """Вернуть (хэш, число строк) результата запроса; порядок строк не влияет на хэш."""
    proc = subprocess.run(
        ["clickhouse-client", "--query", sql, "--format", "TabSeparated"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"запрос упал:\n{proc.stderr}")
    rows = sorted(proc.stdout.splitlines())
    digest = hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
    return digest, len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="путь к SQL-файлу baseline")
    ap.add_argument("--b", required=True, help="путь к SQL-файлу оптимизированного запроса")
    args = ap.parse_args()

    ha, na = fetch_sorted_hash(pathlib.Path(args.a).open(encoding="utf-8").read())
    hb, nb = fetch_sorted_hash(pathlib.Path(args.b).open(encoding="utf-8").read())

    print(f"A: {na:,} строк  {ha[:16]}")
    print(f"B: {nb:,} строк  {hb[:16]}")
    if ha == hb:
        print("\n✅ ЭКВИВАЛЕНТНЫ — результаты идентичны. Ускорение можно фиксировать.")
        sys.exit(0)
    print("\n❌ НЕ ЭКВИВАЛЕНТНЫ — оптимизация изменила ответ. Это баг, а не победа.")
    sys.exit(1)


if __name__ == "__main__":
    main()
