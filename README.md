# dataeng-lab4 — Apache ZooKeeper + координатор ДЗ №3

Репозиторий демонстрирует этапы 1–3 плана из `docs/plan.md`:
ансамбль из трёх серверов ZooKeeper 3.9.6, проверку кворума/ролей/эфемерного
узла и интеграцию ZooKeeper с координатором расчёта пар навыков из ДЗ №3.

**Вариант 9 — «Мобильная разработка».** Правило отбора вакансий и нормализация
навыков берутся из `src/common/variant.py` — того же файла, что и в ДЗ №3.

## Структура

```text
docker-compose.yml          # 3 сервера ZooKeeper (корневой compose-проект)
infra/docker-compose.yml    # RustFS / S3 (отдельный compose-проект)
configs/pipeline.env        # рабочая конфигурация (не коммитится)
scripts/                    # setup_storage.py, upload_dataset.py (из ДЗ №3)
src/mapreduce/main.py       # MapReduce из ДЗ №3 + режим --coordinator-id
src/mapreduce/zk_coordinator.py  # выбор лидера, epoch, чекпоинты, запись результата
coordinator.py              # тонкая обёртка: python coordinator.py --id 1|2
logs/                       # журналы этапа 2 и обоих прогонов координаторов
data/results/mr/            # CSV-результаты координаторов (не коммитятся)
```

## Этап 1. Подъём окружения

ZooKeeper и RustFS — **два разных compose-проекта**, поднимаются отдельно:

```bash
cd D:/VSCode-projects/data-eng/dataeng-lab4
docker compose up -d
docker compose -f infra/docker-compose.yml up -d
docker compose ps
```

- ZooKeeper: `localhost:2181`, `2182`, `2183` (внутри `zoo1/zoo2/zoo3:2181`).
- RustFS S3 API: <http://localhost:9000>, консоль: <http://localhost:9001/rustfs/console/>.
- Java на хосте не нужна: образ `zookeeper:3.9.6` содержит JRE.

Датасет (808 МБ) в git не коммитится. Скрипты ожидают его по пути из
`LOCAL_INPUT` в `configs/pipeline.env`, то есть:

```text
data/raw/it_vacancies_2024.csv
```

Пути в `pipeline.env` относительные и разрешаются от **текущей папки запуска**,
поэтому все команды ниже выполняются из корня репозитория.

```bash
python scripts/setup_storage.py
python scripts/upload_dataset.py
python scripts/upload_dataset.py --verify
```

## Этап 2. Проверка ансамбля

Команды и их вывод сохранены в `logs/stage2_*.txt`; всё можно повторить через
`powershell -File scripts\stage2_capture.ps1`:

```bash
docker exec zoo1 zkServer.sh status
docker exec zoo2 zkServer.sh status
docker exec zoo3 zkServer.sh status

docker compose stop zoo2
docker exec zoo1 timeout 15 zkCli.sh -server localhost:2181 ls /

docker compose stop zoo3
docker exec zoo1 timeout 15 zkCli.sh -server localhost:2181 ls /

docker compose start zoo2 zoo3
```

Эфемерный узел:

```bash
echo -e 'create -e /test_stage2 "hello"\nquit' | docker exec -i zoo2 zkCli.sh -server localhost:2181
echo -e 'ls /\nquit' | docker exec -i zoo1 zkCli.sh -server localhost:2181
```

## Этап 3. Координатор с ZooKeeper

Воспроизведение обоих прогонов (POSIX-окружение — WSL2; там есть
`SIGSTOP`/`SIGCONT`). Скрипт сам дожидается, пока первый координатор выиграет
выборы, и только потом запускает второго:

```bash
cd /mnt/d/VSCode-projects/data-eng/dataeng-lab4
bash scripts/run_failover.sh 0   # FENCING=0 -> logs/run1_*
bash scripts/run_failover.sh 1   # FENCING=1 -> logs/run2_*
```

Координатор сам останавливает лидера после `PAUSE_AFTER_CHUNK` чекпоинта
(`signal.SIGSTOP`) и возобновляет его через `PAUSE_SECONDS` секунд
(дочерний процесс отправляет `SIGCONT`). Тайм-аут сессии — 10 с, поэтому
запасной становится лидером примерно через 10–13 с. Каждый координатор пишет
свой лог в `logs/runN_coordM.log`; stdout дополнительно дублируется в
`logs/runN_stdout_coordM.log`.

Ручной запуск (если нужен один координатор):

```bash
python coordinator.py --id 1 --reset
python coordinator.py --id 2
```

Тот же режим доступен через основной CLI ДЗ №3:

```bash
python src/mapreduce/main.py --coordinator-id 1 --reset-zk
```

Итоговые measurements и список артефактов — в `report.md` и
`logs/stage3_summary.txt`.

### Что пишется в логи

- PID, роль, epoch и состояние сессии ZooKeeper;
- номер и размер каждого чекпоинта;
- время смены лидера (от паузы до получения лидерства запасным);
- контрольные числа и результат сверки с ДЗ №3;
- версия узла `/result` после записи.

Контрольные числа ДЗ №3 для сверки:

| Показатель | Ожидаемое значение |
|---|---:|
| вакансий варианта | 4176 |
| из них с навыками | 3093 |
| уникальные k=1/2/3 | 1138 / 14089 / 73431 |
| вхождения k=1/2/3 | 17153 / 55642 / 153808 |

В `FENCING=0` возобновившийся лидер делает вторую запись: `/result version=2`.
В `FENCING=1` его запись отклоняется с `BadVersionError`, а `/result` остаётся
с версией 1 и от запасного координатора.
