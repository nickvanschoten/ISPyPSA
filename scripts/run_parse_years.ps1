param([Parameter(Mandatory)][string]$Years)
$env:ISP_PARSE_YEARS = $Years
Set-Location (Split-Path -Parent $PSScriptRoot)
$python = Join-Path (Get-Location) '.venv\Scripts\python.exe'
& $python scripts/parse_2026_final_traces.py
exit $LASTEXITCODE
