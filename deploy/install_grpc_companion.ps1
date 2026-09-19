#Requires -Version 5.1
# 将已安装的独立 gRPC 实例注册为 BIG_CONVERT 的伴随任务，不修改交易权限。
param(
    [Parameter(Mandatory=$true)][string]$InstanceDir,
    [Parameter(Mandatory=$true)][string]$ConfigPath,
    [Parameter(Mandatory=$true)][string]$QmtPythonDir,
    [string]$TaskName = 'qmt-grpc-bigqmt',
    [string]$NativePython = ''
)
$ErrorActionPreference = 'Stop'
if ($TaskName -notmatch '^[A-Za-z0-9_-]+$') { throw 'Invalid task name' }
$instancePython = Join-Path $InstanceDir '.venv\Scripts\python.exe'
$localConfig = Join-Path $QmtPythonDir 'bigqmt_signal_trader_local_config.py'
foreach ($required in @($instancePython, $ConfigPath, $localConfig)) {
    if (-not (Test-Path $required)) { throw "Missing required file: $required" }
}
if ($NativePython -and -not (Test-Path $NativePython)) { throw 'Native Python does not exist' }
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($task -and $task.State -eq 'Running') {
    throw 'Stop this independent task before replacing its launcher'
}
$backup = Join-Path $InstanceDir ('companion-backup-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup | Out-Null
Copy-Item $localConfig (Join-Path $backup 'bigqmt_signal_trader_local_config.py')
if ($task) { Export-ScheduledTask -TaskName $TaskName | Set-Content (Join-Path $backup 'task.xml') -Encoding Unicode }
$launcher = Join-Path $InstanceDir 'start-bigqmt-grpc.ps1'
if (Test-Path $launcher) { Copy-Item $launcher (Join-Path $backup 'start-bigqmt-grpc.ps1') }
$configDir = Split-Path -Parent $ConfigPath
$logDir = Join-Path $configDir 'logs'
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

function Quote-PS([string]$Value) {
    # 生成单引号 PowerShell 字符串，路径中的字符不能成为命令。
    return "'" + $Value.Replace("'", "''") + "'"
}

function Quote-Native([string]$Value) {
    # 文件路径不经过 shell，显式保留传给 Windows 原生命令的参数边界。
    return '"' + $Value.Replace('"', '\"') + '"'
}

$arguments = '-u -m qmt_grpc.bootstrap.bigqmt_companion --config ' + (Quote-Native $ConfigPath)
if ($NativePython) {
    $exporter = Join-Path $InstanceDir 'export_etf_reference.py'
    Copy-Item (Join-Path $PSScriptRoot 'export_etf_reference.py') $exporter -Force
    $arguments += ' --reference-python ' + (Quote-Native $NativePython) + ' --reference-exporter ' + (Quote-Native $exporter)
}
$lines = @(
    '# 由 BIG_CONVERT 触发；伴随进程负责就绪等待、分类刷新和异常重试。',
    '$ErrorActionPreference = ''Stop''',
    '$env:PYTHONIOENCODING = ''utf-8''',
    '$env:BIGQMT_AUTO_SYNC = ''0''',
    '$env:BIGQMT_FORMULA_ENABLED = ''0''',
    ('$env:BIGQMT_ORDER_ID_DB = ' + (Quote-PS (Join-Path $configDir 'order_ids.sqlite3'))),
    ('Set-Location ' + (Quote-PS $InstanceDir)),
    ('$process = Start-Process -FilePath ' + (Quote-PS $instancePython) + ' -ArgumentList ' + (Quote-PS $arguments) + ' -NoNewWindow -Wait -PassThru -RedirectStandardOutput ' + (Quote-PS (Join-Path $logDir 'companion.stdout.log')) + ' -RedirectStandardError ' + (Quote-PS (Join-Path $logDir 'companion.log'))),
    'exit $process.ExitCode'
)
$lines | Set-Content -Path $launcher -Encoding UTF8
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $launcher + '"') -WorkingDirectory $InstanceDir
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $action -Principal $principal -Settings $settings -Force | Out-Null
$content = [System.IO.File]::ReadAllText($localConfig)
$content = [regex]::Replace($content, '(?ms)^# BEGIN BIGQMT GRPC COMPANION\r?\n.*?^# END BIGQMT GRPC COMPANION\r?\n?', '')
$content += "`r`n# BEGIN BIGQMT GRPC COMPANION`r`nBIGQMT_GRPC_COMPANION = {`"enabled`": True, `"task_name`": `"$TaskName`"}`r`n# END BIGQMT GRPC COMPANION`r`n"
[System.IO.File]::WriteAllText($localConfig, $content, [System.Text.UTF8Encoding]::new($false))
Write-Output "Companion configured: $TaskName; backup: $backup"
