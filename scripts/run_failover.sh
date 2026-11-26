#!/usr/bin/env bash
# Запуск двух координаторов с паузой лидера посреди расчёта.
# Использование (из WSL):
#   bash scripts/run_failover.sh 0   # FENCING=0, run1
#   bash scripts/run_failover.sh 1   # FENCING=1, run2
set -uo pipefail

FENCING=${1:-0}
case "$FENCING" in
  0) RUN_NO=1 ;;
  1) RUN_NO=2 ;;
  *) echo "Использование: $0 0|1"; exit 2 ;;
esac

cd /mnt/d/VSCode-projects/data-eng/dataeng-lab4
mkdir -p logs data/results/mr

echo "=== run$RUN_NO FENCING=$FENCING ===" | tee -a logs/failover_runs.txt

export FENCING=$FENCING
export PYTHONUNBUFFERED=1

python3 coordinator.py --id 1 --reset > "logs/run${RUN_NO}_stdout_coord1.log" 2>&1 &
P1=$!

# Второй координатор стартуем только после того, как первый реально выиграл
# выборы и закрепил свой ephemeral-узел в /coordinator/election.
for _ in $(seq 1 120); do
  if grep -q "Лидерство получено" "logs/run${RUN_NO}_coord1.log" 2>/dev/null; then
    break
  fi
  sleep 0.5
done
sleep 1

python3 coordinator.py --id 2 > "logs/run${RUN_NO}_stdout_coord2.log" 2>&1 &
P2=$!

wait "$P1"; RC1=$?
wait "$P2"; RC2=$?

echo "run$RUN_NO завершён: coord1 rc=$RC1, coord2 rc=$RC2" | tee -a logs/failover_runs.txt
