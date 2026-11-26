"""Координатор ДЗ №4: выбор лидера через ZooKeeper + чекпоинты + epoch fencing.

Использует проверенную MapReduce-реализацию ДЗ №3:
    map_phase / reduce_phase / read_header / write_results / md5_of

Запуск:
    python coordinator.py --id 1 --reset
    python coordinator.py --id 2
    python src/mapreduce/main.py --coordinator-id 1 --reset-zk

Переменные конфигурации читаются из configs/pipeline.env (см. common.config):
    ZK_HOSTS, ZK_SESSION_TIMEOUT, FENCING, PAUSE_AFTER_CHUNK, PAUSE_SECONDS
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import signal
import sys
import time
import traceback
import zlib
from collections import Counter
from math import comb
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from kazoo.client import KazooClient  # noqa: E402
from kazoo.exceptions import (  # noqa: E402
    BadVersionError,
    ConnectionLoss,
    NodeExistsError,
    NoNodeError,
    SessionExpiredError,
)
from kazoo.recipe.election import Election  # noqa: E402

from common.config import load_settings  # noqa: E402
from common.s3 import make_s3_client  # noqa: E402
from mapreduce.mapper import NEEDED_COLUMNS, ChunkTask  # noqa: E402
from mapreduce.reader import plan_chunks  # noqa: E402
from mapreduce.main import (  # noqa: E402
    human,
    map_phase,
    md5_of,
    read_header,
    reduce_phase,
    write_results,
)

# --- ZooKeeper: пути и настройки по умолчанию ---
ZK_COORDINATOR_PATH = "/coordinator"
ZK_ELECTION_PATH = "/coordinator/election"
ZK_EPOCH_PATH = "/coordinator/epoch"
ZK_PAUSE_FLAG_PATH = "/coordinator/pause_flag"
ZK_CHECKPOINTS_PATH = "/checkpoints"
ZK_RESULT_PATH = "/result"

DEFAULT_ZK_HOSTS = "localhost:2181,localhost:2182,localhost:2183"

# Контрольные числа ДЗ №3 (README + data/results/mr/summary.csv).
EXPECTED_LAB3 = {
    "total_rows": 229388,
    "total_matched": 4176,
    "total_with_skills": 3093,
    "unique_k1": 1138,
    "unique_k2": 14089,
    "unique_k3": 73431,
    "total_k1": 17153,
    "total_k2": 55642,
    "total_k3": 153808,
}

_LOG_FH = None


def log(msg: str) -> None:
    """Дублирует сообщение в stdout и в logs/runN_coordM.log."""
    line = f"[{time.strftime('%H:%M:%S.%f')[:-3]}] {msg}"
    print(line, flush=True)
    if _LOG_FH is not None:
        _LOG_FH.write(line + "\n")
        _LOG_FH.flush()


def setup_logging(my_id: str, fencing: bool) -> None:
    global _LOG_FH
    run_no = 2 if fencing else 1
    path = REPO_ROOT / "logs" / f"run{run_no}_coord{my_id}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    _LOG_FH = path.open("w", encoding="utf-8", newline="\n")
    log(f"Лог запуска: {path.relative_to(REPO_ROOT)}")


def write_artifact(name: str, text: str) -> None:
    """Небольшой текстовый артефакт для отчёта."""
    path = REPO_ROOT / "logs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")
    log(f"Артефакт сохранён: {path.relative_to(REPO_ROOT)}")


class LogAdapter:
    """Адаптер под интерфейс Transcript, который использует map_phase."""

    def log(self, *args, sep=" ") -> None:
        log(sep.join(str(a) for a in args))


def read_config(args: argparse.Namespace) -> dict:
    settings = load_settings()
    extra = settings.extra

    def as_int(key: str, default: int) -> int:
        try:
            return int(extra.get(key, default))
        except (TypeError, ValueError):
            return default

    fencing = extra.get("FENCING", "0") == "1"
    if getattr(args, "fencing", None) is not None:
        fencing = str(args.fencing) == "1"

    pause_after = as_int("PAUSE_AFTER_CHUNK", -1)
    if getattr(args, "pause_after_chunk", None) is not None:
        pause_after = args.pause_after_chunk

    pause_seconds = as_int("PAUSE_SECONDS", 40)
    if getattr(args, "pause_seconds", None) is not None:
        pause_seconds = args.pause_seconds

    return {
        "settings": settings,
        "zk_hosts": extra.get("ZK_HOSTS", DEFAULT_ZK_HOSTS),
        "session_timeout": as_int("ZK_SESSION_TIMEOUT", 10),
        "fencing": fencing,
        "pause_after": pause_after,
        "pause_seconds": pause_seconds,
    }


def zk_state_listener(state) -> None:
    log(f"Состояние сессии ZooKeeper: {state}")


def connect_zk(cfg: dict) -> KazooClient:
    zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["session_timeout"])
    zk.add_listener(zk_state_listener)
    zk.start(timeout=30)
    log(
        f"Подключено к ZooKeeper: hosts={cfg['zk_hosts']}, "
        f"session_timeout={cfg['session_timeout']} с"
    )
    return zk


def wait_connected(zk: KazooClient, timeout: float = 30.0) -> None:
    """Ждём, пока kazoo восстановит сессию после SIGSTOP/SIGCONT."""
    started = time.perf_counter()
    while not zk.connected:
        if time.perf_counter() - started > timeout:
            raise SessionExpiredError("не удалось восстановить сессию ZooKeeper")
        time.sleep(0.2)


def ensure_paths(zk: KazooClient) -> None:
    for path in (
        ZK_COORDINATOR_PATH,
        ZK_ELECTION_PATH,
        ZK_EPOCH_PATH,
        ZK_CHECKPOINTS_PATH,
    ):
        zk.ensure_path(path)


def reset_state(zk: KazooClient) -> None:
    """Чистит состояние перед запуском новой демонстрации."""
    for path in (
        ZK_CHECKPOINTS_PATH,
        ZK_RESULT_PATH,
        ZK_EPOCH_PATH,
        ZK_PAUSE_FLAG_PATH,
        ZK_ELECTION_PATH,
    ):
        if zk.exists(path):
            zk.delete(path, recursive=True)
    ensure_paths(zk)
    # ensure_path уже создал узел epoch — просто обнуляем его значение.
    zk.set(ZK_EPOCH_PATH, b"0")
    # Указатель результата создаём заранее с version=0. Тогда первая запись
    # запасного даст version=1, а вторая запись старого лидера — version=2,
    # как требуется в задании.
    zk.create(
        ZK_RESULT_PATH,
        json.dumps({"status": "initialized"}).encode("utf-8"),
        makepath=True,
    )
    log("Состояние ZooKeeper сброшено: /checkpoints, /result(version=0), epoch, pause_flag")


def bump_epoch(zk: KazooClient) -> int:
    """Атомарно увеличивает epoch при получении лидерства (CAS по версии)."""
    zk.ensure_path(ZK_EPOCH_PATH)
    while True:
        try:
            data, stat = zk.get(ZK_EPOCH_PATH)
            current = int((data or b"0").decode("utf-8"))
            new_epoch = current + 1
            zk.set(ZK_EPOCH_PATH, str(new_epoch).encode("utf-8"), version=stat.version)
            log(f"Получена новая эпоха: {current} -> {new_epoch} (CAS version={stat.version})")
            return new_epoch
        except (BadVersionError, NoNodeError):
            # Кто-то уже обновил epoch — перечитаем и повторим.
            continue
        except (ConnectionLoss, SessionExpiredError):
            wait_connected(zk)


def try_mark_pause(zk: KazooClient) -> bool:
    """Флаг паузы создаётся только первым лидером — второй уже не pause."""
    try:
        zk.create(ZK_PAUSE_FLAG_PATH, f"{time.time():.6f}".encode("ascii"))
        return True
    except NodeExistsError:
        return False


def pause_with_autoresume(zk: KazooClient, cfg: dict) -> None:
    """Останавливает лидера SIGSTOP и гарантированно снимает паузу SIGCONT.

    Дочерний процесс не останавливается: он спит PAUSE_SECONDS и отправляет
    SIGCONT родителю. Это моделирует приостановку лидера посреди расчёта:
    heartbeat клиента тоже останавливается, сессия истекает, эфемерный узел
    лидерства исчезает.
    """
    if not try_mark_pause(zk):
        log("Пауза уже выполнялась в этом запуске — пропускаем")
        return

    pid = os.getpid()
    started = time.perf_counter()
    wall = time.time()
    log(
        f"ПАУЗА ЛИДЕРА: PID={pid}, после чекпоинта, "
        f" autorеsume через {cfg['pause_seconds']} с"
    )
    write_artifact(
        "pause_event.txt",
        f"pause_wall_time={wall:.6f}\npid={pid}\n"
        f"session_timeout={cfg['session_timeout']}\n"
        f"resume_after_seconds={cfg['pause_seconds']}\n",
    )

    child = os.fork()
    if child == 0:
        time.sleep(cfg["pause_seconds"])
        try:
            os.kill(pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
        os._exit(0)

    os.kill(pid, signal.SIGSTOP)
    log(f"Лидер возобновлён через {time.perf_counter() - started:.2f} с")


def log_leader_change(zk: KazooClient) -> None:
    """Измеряет время от паузы старого лидера до получения лидерства запасным."""
    try:
        data, _ = zk.get(ZK_PAUSE_FLAG_PATH)
        pause_ts = float(data.decode("ascii"))
        delta = time.time() - pause_ts
        msg = f"ВРЕМЯ СМЕНЫ ЛИДЕРА: {delta:.2f} с (пауза -> новый лидер)"
        log(msg)
        write_artifact("leader_change.txt", msg)
    except NoNodeError:
        log("Пауза не была зафиксирована — время смены лидера не измеряем")


def save_checkpoint(zk: KazooClient, idx: int, result) -> None:
    path = f"{ZK_CHECKPOINTS_PATH}/chunk_{idx:03d}"
    blob = zlib.compress(pickle.dumps(result, protocol=5), level=6)
    if len(blob) > 900_000:
        log(f"ВНИМАНИЕ: чекпоинт чанка {idx} большой ({len(blob)} байт), лимит znode ~1 МБ")

    for attempt in range(1, 11):
        try:
            try:
                zk.create(path, blob, makepath=True)
            except NodeExistsError:
                zk.set(path, blob)
            log(
                f"Чекпоинт чанка {idx:02d} сохранён: {len(blob)} байт (pickle+zlib), "
                f"строк={result.rows}, вакансий={result.matched}"
            )
            return
        except (ConnectionLoss, SessionExpiredError) as exc:
            log(
                f"Чекпоинт чанка {idx}: сессия возобновляется "
                f"({type(exc).__name__}), попытка {attempt}/10"
            )
            wait_connected(zk)
    raise RuntimeError(f"Не удалось сохранить чекпоинт чанка {idx}")


def load_checkpoints(zk: KazooClient) -> dict[int, object]:
    checkpoints: dict[int, object] = {}
    try:
        children = zk.get_children(ZK_CHECKPOINTS_PATH)
    except NoNodeError:
        return checkpoints

    for child in sorted(children):
        if not child.startswith("chunk_"):
            continue
        try:
            idx = int(child.split("_", 1)[1])
        except ValueError:
            continue
        try:
            data, _ = zk.get(f"{ZK_CHECKPOINTS_PATH}/{child}")
            checkpoints[idx] = pickle.loads(zlib.decompress(data))
        except Exception as exc:  # noqa: BLE001
            log(f"Не удалось прочитать чекпоинт {child}: {type(exc).__name__}: {exc}")
    return checkpoints


def write_result(
    zk: KazooClient,
    result_data: dict,
    fencing: bool,
    my_id: str,
    my_epoch: int,
) -> int:
    """Публикует результат; при fencing проверяет эпоху и версию znode (CAS)."""
    data = json.dumps(result_data, ensure_ascii=False, indent=2).encode("utf-8")

    for attempt in range(1, 6):
        try:
            if fencing:
                epoch_data, _ = zk.get(ZK_EPOCH_PATH)
                current_epoch = int((epoch_data or b"0").decode("utf-8"))
                log(f"ПРОВЕРКА ЭПОХИ: моя={my_epoch}, в ZooKeeper={current_epoch}")
                if my_epoch != current_epoch:
                    msg = (
                        f"epoch {my_epoch} устарела (текущая {current_epoch})"
                    )
                    log(f"BadVersionError: запись отклонена — {msg}")
                    raise BadVersionError(msg)

            try:
                _, stat = zk.get(ZK_RESULT_PATH)
                expected_version = stat.version
            except NoNodeError:
                # На всякий случай восстанавливаем указатель, если его удалили.
                zk.create(ZK_RESULT_PATH, json.dumps({"status": "initialized"}).encode(), makepath=True)
                _, stat = zk.get(ZK_RESULT_PATH)
                expected_version = stat.version
            zk.set(ZK_RESULT_PATH, data, version=expected_version)
            new_version = expected_version + 1
            action = "записан (CAS)"

            log(
                f"Результат {action}: coordinator_id={my_id}, epoch={my_epoch}, "
                f"FENCING={int(fencing)}, /result version={new_version}"
            )
            write_artifact(
                f"result_version_run{2 if fencing else 1}.txt",
                f"coordinator_id={my_id}\nepoch={my_epoch}\n"
                f"fencing={int(fencing)}\nresult_version={new_version}\n",
            )

            final_data, final_stat = zk.get(ZK_RESULT_PATH)
            final = json.loads(final_data.decode("utf-8"))
            log(
                f"Финальная проверка /result: version={final_stat.version}, "
                f"writer={final['coordinator_id']}, epoch={final['epoch']}"
            )
            write_artifact(
                f"result_final_run{2 if fencing else 1}.txt",
                json.dumps(final, ensure_ascii=False, indent=2),
            )
            return new_version

        except BadVersionError:
            if fencing:
                raise
            log("Конкурентная версия /result, повторяем запись без защиты")
        except (ConnectionLoss, SessionExpiredError) as exc:
            log(f"ZK временно недоступен ({type(exc).__name__}), повтор {attempt}/5")
            wait_connected(zk)
        time.sleep(1)

    raise RuntimeError("Не удалось записать результат после 5 попыток")


def run_computation(
    zk: KazooClient,
    my_id: str,
    cfg: dict,
    my_epoch: int,
) -> dict:
    settings = cfg["settings"]
    fencing = cfg["fencing"]
    adapter = LogAdapter()

    client = make_s3_client(settings)
    head = client.head_object(Bucket=settings.s3_bucket, Key=settings.input_object)
    size = head["ContentLength"]
    chunks = plan_chunks(size, settings.chunk_size_mb * 1024 * 1024)
    header = read_header(client, settings)
    columns = {name: header.index(name) for name in NEEDED_COLUMNS}

    log(
        f"Расчёт: объект=s3://{settings.s3_bucket}/{settings.input_object}, "
        f"размер={human(size)}, чанков={len(chunks)}, "
        f"мапперов={settings.workers}, редьюсеров={settings.reducers}"
    )

    checkpoints = load_checkpoints(zk)
    log(f"Загружено чекпоинтов из ZooKeeper: {len(checkpoints)}")

    tasks = [
        ChunkTask(
            chunk=chunk,
            size=size,
            columns=columns,
            reducers=settings.reducers,
            settings=settings,
        )
        for chunk in chunks
    ]
    pending = [task for task in tasks if task.index not in checkpoints]
    results: dict[int, object] = dict(checkpoints)
    saved = 0

    def on_chunk_done(idx: int, result, attempts: int) -> None:
        nonlocal saved
        save_checkpoint(zk, idx, result)
        results[idx] = result
        saved += 1
        if cfg["pause_after"] > 0 and saved == cfg["pause_after"]:
            pause_with_autoresume(zk, cfg)

    if pending:
        log(f"К обработке: {len(pending)} чанков (остальные взяты из чекпоинтов)")
        fresh, retries = map_phase(
            pending,
            workers=settings.workers,
            max_attempts=3,
            fail_chunk=None,
            fail_mode="none",
            t=adapter,
            on_chunk_done=on_chunk_done,
        )
        results.update(fresh)
        log(f"Map-фаза завершена: чанков={len(fresh)}, повторов={retries}")
    else:
        log("Все чанки уже есть в чекпоинтах — Map-фаза пропущена")

    reduced = reduce_phase(results, settings.reducers, adapter)

    total_rows = sum(getattr(r, "rows", 0) for r in results.values())
    total_bad = sum(getattr(r, "bad_rows", 0) for r in results.values())
    total_matched = sum(getattr(r, "matched", 0) for r in results.values())
    total_with_skills = sum(getattr(r, "with_skills", 0) for r in results.values())

    hist: Counter[int] = Counter()
    for result in results.values():
        hist.update(getattr(result, "skill_hist", {}) or {})
    hist_dict = dict(sorted(hist.items()))

    for k in range(1, 4):
        expected = sum(comb(n, k) * v for n, v in hist_dict.items() if n >= k)
        actual = sum(reduced[k].values())
        if expected != actual:
            raise RuntimeError(
                f"Расхождение C(n,{k}) с гистограммой: {actual} != {expected}"
            )

    log(
        f"КОНТРОЛЬНЫЕ ЧИСЛА: строк={total_rows}, плохих={total_bad}, "
        f"вакансий={total_matched}, с навыками={total_with_skills}"
    )
    log(
        "УНИКАЛЬНЫЕ КЛЮЧИ: k1={0}, k2={1}, k3={2} | "
        "ВХОЖДЕНИЯ: k1={3}, k2={4}, k3={5}".format(
            len(reduced[1]), len(reduced[2]), len(reduced[3]),
            sum(reduced[1].values()), sum(reduced[2].values()), sum(reduced[3].values()),
        )
    )

    result_data = {
        "coordinator_id": my_id,
        "epoch": my_epoch,
        "fencing": int(fencing),
        "total_rows": total_rows,
        "total_bad": total_bad,
        "total_matched": total_matched,
        "total_with_skills": total_with_skills,
        "unique_k1": len(reduced[1]),
        "unique_k2": len(reduced[2]),
        "unique_k3": len(reduced[3]),
        "total_k1": sum(reduced[1].values()),
        "total_k2": sum(reduced[2].values()),
        "total_k3": sum(reduced[3].values()),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    diffs = [
        f"{key}: {result_data.get(key)} != {expected}"
        for key, expected in EXPECTED_LAB3.items()
        if result_data.get(key) != expected
    ]
    verdict = "СОВПАДАЮТ" if not diffs else "РАСХОЖДЕНИЕ: " + "; ".join(diffs)
    log(f"СВЕРКА С ДЗ №3: {verdict}")
    write_artifact(
        f"control_numbers_run{2 if fencing else 1}.txt",
        json.dumps(result_data, ensure_ascii=False, indent=2)
        + f"\n\nСВЕРКА С ДЗ №3: {verdict}\n"
        + "Ожидаемые значения: "
        + ", ".join(f"{k}={v}" for k, v in EXPECTED_LAB3.items())
        + "\n",
    )

    tag = f"_coord{my_id}_f{int(fencing)}"
    paths = write_results(reduced, hist_dict, Path(settings.output_dir), tag)
    for name, path in paths.items():
        log(f"  {name:<18} {path} MD5={md5_of(path)}")

    write_result(zk, result_data, fencing, my_id, my_epoch)
    return result_data


def leader_work(zk: KazooClient, cfg: dict, my_id: str) -> None:
    log("Лидерство получено через kazoo.recipe.election.Election")
    log_leader_change(zk)
    epoch = bump_epoch(zk)
    try:
        run_computation(zk, my_id, cfg, epoch)
    except BadVersionError as exc:
        # Ожидаемый исход для FENCING=1 у возобновившегося старого лидера.
        log(f"BadVersionError: запись отклонена, результат остался у нового лидера ({exc})")
        write_artifact(
            "fencing_rejection.txt",
            f"coordinator_id={my_id}\nmy_epoch={epoch}\nreason={exc}\n",
        )
    except Exception:
        log("Ошибка в работе лидера:\n" + traceback.format_exc())
        raise


def run_coordinator(args: argparse.Namespace) -> int:
    cfg = read_config(args)
    setup_logging(args.id, cfg["fencing"])
    log(
        f"=== Координатор id={args.id} стартует: PID={os.getpid()}, "
        f"FENCING={int(cfg['fencing'])}, "
        f"PAUSE_AFTER_CHUNK={cfg['pause_after']}, PAUSE_SECONDS={cfg['pause_seconds']} ==="
    )

    zk = connect_zk(cfg)
    try:
        if args.reset:
            reset_state(zk)
        election = Election(zk, ZK_ELECTION_PATH, identifier=args.id)
        log(f"Участвую в выборах лидера: {ZK_ELECTION_PATH}")
        try:
            election.run(leader_work, zk, cfg, args.id)
        except Exception as exc:
            # После истечения сессии старый лидер может не суметь снять lock —
            # это нормально: его ephemeral-узел уже удалён ZooKeeper.
            log(f"Завершение Election: {type(exc).__name__}: {exc}")
    except Exception:
        log("Фатальная ошибка координатора:\n" + traceback.format_exc())
    finally:
        zk.stop()
        zk.close()
        log(f"=== Координатор id={args.id} завершил работу ===")
        if _LOG_FH is not None:
            _LOG_FH.close()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Координатор с ZooKeeper (ДЗ №4)")
    parser.add_argument("--id", required=True, help="ID координатора (1 или 2)")
    parser.add_argument("--reset", action="store_true", help="сбросить состояние ZK")
    parser.add_argument("--fencing", choices=["0", "1"], default=None)
    parser.add_argument("--pause-after-chunk", type=int, default=None)
    parser.add_argument("--pause-seconds", type=int, default=None)
    return parser.parse_args(argv)


def main() -> int:
    return run_coordinator(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
