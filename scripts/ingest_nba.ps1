[CmdletBinding()]
param(
    [string]$Season,
    [string]$Since,
    [string]$Until,
    [string]$ApiBase = "http://127.0.0.1:8000"
)

$ErrorActionPreference = "Stop"
$EtNow = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Eastern Standard Time")
if (-not $Season) {
    $StartYear = if ($EtNow.Month -ge 7) { $EtNow.Year } else { $EtNow.Year - 1 }
    $Season = "{0}-{1:D2}" -f $StartYear, (($StartYear + 1) % 100)
}
if (-not $Until) { $Until = $EtNow.ToString("yyyy-MM-dd") }
if (-not $Since) { $Since = $Until }

Invoke-RestMethod -Method Post -Uri "$ApiBase/db/teams/load"
Invoke-RestMethod -Method Post -Uri "$ApiBase/db/schedule/load?season=$([uri]::EscapeDataString($Season))"
$Query = "since=$Since&until=$Until&season=$([uri]::EscapeDataString($Season))&include_team_stats=true"
Invoke-RestMethod -Method Post -Uri "$ApiBase/db/games/ingest?$Query"
