$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { throw "Run scripts\setup_windows.ps1 first." }

Push-Location (Join-Path $ProjectRoot "backend")
try {
    $DatabaseUrls = @(& $Python -c "from app.core.config import settings; print(settings.DATABASE_URL); print(settings.ML_DATABASE_URL)") | Select-Object -Unique
    foreach ($DatabaseUrl in $DatabaseUrls) {
        $UriValue = $DatabaseUrl -replace '^postgresql\+[^:]+:', 'postgresql:'
        $DatabaseHost = ([Uri]$UriValue).Host
        if ($DatabaseHost -notin @("127.0.0.1", "localhost", "::1")) {
            throw "Refusing to migrate remote database host '$DatabaseHost'. Run migrations against local PostgreSQL only."
        }
        $env:ALEMBIC_DATABASE_URL = $DatabaseUrl
        & $Python -m alembic upgrade head
        if ($LASTEXITCODE -ne 0) { throw "Alembic migration failed." }
    }
} finally {
    Remove-Item Env:ALEMBIC_DATABASE_URL -ErrorAction SilentlyContinue
    Pop-Location
}
