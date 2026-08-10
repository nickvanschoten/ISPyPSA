param([ValidateSet('Solar', 'Wind')][string]$Kind, [int]$StartYear = 2011, [int]$EndYear = 2025)
if (-not $Kind) { $Kind = 'Solar' }

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$base = Join-Path $root 'iasr inputs\2026 ISP Final'
$items = foreach ($kind in @($Kind)) {
    $folder = Join-Path $base "2026 ISP $kind traces"
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    foreach ($year in ($StartYear..$EndYear)) {
        [pscustomobject]@{
            Kind = $kind
            Year = $year
            Url = "https://publicfileupload.blob.core.windows.net/sitefiles/ISP%20$kind%20Traces%20r$year.zip"
            Out = Join-Path $folder "ISP $kind Traces r$year.zip"
        }
    }
}

foreach ($item in $items) {
    $head = Invoke-WebRequest -Uri $item.Url -Method Head -UseBasicParsing
    $expected = [int64]$head.Headers['Content-Length']
    if (Test-Path -LiteralPath $item.Out) {
        $existing = (Get-Item -LiteralPath $item.Out).Length
        if ($existing -eq $expected) {
            continue
        }
    }
    $temp = "$($item.Out).partial"
    & curl.exe --fail --location --retry 5 --retry-delay 5 --silent --show-error --output $temp $item.Url
    if ($LASTEXITCODE -ne 0) { throw "Download failed: $($item.Url)" }
    $length = (Get-Item -LiteralPath $temp).Length
    if ($length -ne $expected) { throw "Short download for $($item.Url): $length / $expected" }
    Move-Item -LiteralPath $temp -Destination $item.Out -Force
}
