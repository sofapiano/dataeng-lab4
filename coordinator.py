"""Точка входа координатора ZooKeeper (ДЗ №4).

Реализация лежит в src/mapreduce/zk_coordinator.py и переиспользует
MapReduce-функции ДЗ №3 из src/mapreduce/main.py.

Запуск:
    python coordinator.py --id 1 --reset
    python coordinator.py --id 2
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mapreduce.zk_coordinator import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
