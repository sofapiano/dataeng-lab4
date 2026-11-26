# Этап 2: фиксация ролей, потери серверов и эфемерного узла.
# Запуск из корня репозитория:
#   powershell -File scripts\stage2_capture.ps1

$ErrorActionPreference = 'Continue'
$logDir = Join-Path (Get-Location) 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log($file, $lines) {
    $path = Join-Path $logDir $file
    $header = @(
        "=== $file ===",
        "Дата: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
        ''
    )
    $header + $lines | Set-Content -Path $path -Encoding UTF8
}

function Run-And-Capture($block) {
    return (& $block 2>&1 | ForEach-Object { $_.ToString() })
}

# --- 1. Роли серверов ---
$roles = @()
$roles += "Время: $(Get-Date -Format 'HH:mm:ss')"
foreach ($c in @('zoo1','zoo2','zoo3')) {
    $roles += "--- $c : docker exec $c zkServer.sh status ---"
    $roles += (Run-And-Capture { docker exec $c zkServer.sh status })
    $roles += ''
}
Write-Log 'stage2_roles.txt' $roles

# --- 2. Один сервер остановлен, кворум есть ---
$oneDown = @()
docker compose stop zoo2 2>&1 | ForEach-Object { $oneDown += $_.ToString() }
$oneDown += "Время после остановки zoo2: $(Get-Date -Format 'HH:mm:ss')"
foreach ($c in @('zoo1','zoo3')) {
    $oneDown += "--- чтение через $c ---"
    $oneDown += (Run-And-Capture { docker exec $c timeout 15 zkCli.sh -server localhost:2181 ls / })
}
$oneDown += "--- docker compose ps ---"
$oneDown += (Run-And-Capture { docker compose ps })
Write-Log 'stage2_one_down.txt' $oneDown

# --- 3. Два сервера остановлены, кворума нет ---
$noQuorum = @()
docker compose stop zoo3 2>&1 | ForEach-Object { $noQuorum += $_.ToString() }
$noQuorum += "Время после остановки zoo3: $(Get-Date -Format 'HH:mm:ss')"
$noQuorum += '--- попытка чтения через zoo1 (должна завершиться ошибкой/таймаутом) ---'
$noQuorum += (Run-And-Capture { docker exec zoo1 timeout 15 zkCli.sh -server localhost:2181 ls / })
$noQuorum += "timeout exit code: $LASTEXITCODE"
$noQuorum += "--- docker compose ps ---"
$noQuorum += (Run-And-Capture { docker compose ps })
Write-Log 'stage2_no_quorum.txt' $noQuorum

# --- 4. Возврат серверов и время восстановления ---
$restore = @()
$t0 = Get-Date
docker compose start zoo2 zoo3 2>&1 | ForEach-Object { $restore += $_.ToString() }
$ok = $false
for ($i = 0; $i -lt 30; $i++) {
    $out = Run-And-Capture { docker exec zoo1 timeout 5 zkCli.sh -server localhost:2181 ls / }
    if (($out -join "`n") -match '\[.*zookeeper') { $ok = $true; break }
    Start-Sleep -Seconds 1
}
$elapsed = ((Get-Date) - $t0).TotalSeconds
$restore += "Ансамбль снова отвечает: $ok"
$restore += "Время восстановления после start: $([math]::Round($elapsed, 2)) с"
$restore += "--- docker compose ps ---"
$restore += (Run-And-Capture { docker compose ps })
Write-Log 'stage2_restore.txt' $restore

# --- 5. Эфемерный узел ---
$eph = @()
$eph += "Время создания: $(Get-Date -Format 'HH:mm:ss')"
$eph += '--- create -e /test_stage2 "hello" в сессии zoo2, затем quit ---'
$createCmd = "create -e /test_stage2 `"hello`"`nquit`n"
$eph += ($createCmd | docker exec -i zoo2 zkCli.sh -server localhost:2181 2>&1 | ForEach-Object { $_.ToString() })
$eph += "--- новая сессия: ls / через zoo1 ---"
$lsCmd = "ls /`nquit`n"
$eph += ($lsCmd | docker exec -i zoo1 zkCli.sh -server localhost:2181 2>&1 | ForEach-Object { $_.ToString() })
Write-Log 'stage2_ephemeral.txt' $eph

Write-Host "Готово. Файлы в $logDir"
