[CmdletBinding()]
param(
    [string]$PythonVersion = "3.13"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv is required. Install it from https://docs.astral.sh/uv/ and rerun this script."
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    uv venv .venv --python $PythonVersion
}
uv pip install --python .venv\Scripts\python.exe -r requirements-dev.txt

Push-Location frontend
try {
    npm ci
    if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
        Copy-Item ".env.example" ".env"
    }
} finally {
    Pop-Location
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Review database URLs before migrating." -ForegroundColor Yellow
}

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    Write-Warning "PostgreSQL client tools are not on PATH. Install PostgreSQL 16+ before creating databases or restoring backup.dump."
}

Write-Host "Environment ready. Next: run scripts\create_local_databases.ps1, scripts\migrate.ps1, then scripts\start_dev.ps1." -ForegroundColor Green
