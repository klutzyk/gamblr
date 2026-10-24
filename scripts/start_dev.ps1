$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { throw "Run scripts\setup_windows.ps1 first." }

$Backend = Start-Process -FilePath $Python -ArgumentList @(
    "-m", "uvicorn", "app.main:app", "--reload", "--host", "127.0.0.1", "--port", "8000"
) -WorkingDirectory (Join-Path $ProjectRoot "backend") -PassThru -WindowStyle Hidden
$Frontend = Start-Process -FilePath "npm.cmd" -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1") -WorkingDirectory (Join-Path $ProjectRoot "frontend") -PassThru -WindowStyle Hidden

Write-Host "Backend PID $($Backend.Id): http://127.0.0.1:8000/docs"
Write-Host "Frontend PID $($Frontend.Id): http://127.0.0.1:5173"
Write-Host "Stop with: Stop-Process -Id $($Backend.Id),$($Frontend.Id)"
