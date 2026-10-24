[CmdletBinding()]
param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 5432,
    [string]$UserName = "postgres",
    [string[]]$DatabaseNames = @("gamblr", "gamblr_ml")
)

$ErrorActionPreference = "Stop"
if ($HostName -notin @("127.0.0.1", "localhost", "::1")) {
    throw "Refusing to create databases on non-local host '$HostName'."
}
if (-not (Get-Command psql -ErrorAction SilentlyContinue) -or
    -not (Get-Command createdb -ErrorAction SilentlyContinue)) {
    throw "PostgreSQL psql and createdb must be available on PATH."
}

foreach ($DatabaseName in $DatabaseNames) {
    if ($DatabaseName -notmatch '^[A-Za-z][A-Za-z0-9_]*$') {
        throw "Unsafe database name '$DatabaseName'."
    }
    $Exists = & psql -h $HostName -p $Port -U $UserName -d postgres -tAc `
        "SELECT 1 FROM pg_database WHERE datname = '$DatabaseName'"
    if ($LASTEXITCODE -ne 0) { throw "Could not query local PostgreSQL." }
    if ($Exists -eq "1") {
        Write-Host "Database '$DatabaseName' already exists; leaving it unchanged."
        continue
    }
    & createdb -h $HostName -p $Port -U $UserName $DatabaseName
    if ($LASTEXITCODE -ne 0) { throw "Could not create database '$DatabaseName'." }
    Write-Host "Created local database '$DatabaseName'." -ForegroundColor Green
}
