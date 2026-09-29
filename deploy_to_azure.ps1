<#
.SYNOPSIS
    Push the local cdec_maps source tree to the Azure Files share used
    by the App Service, and optionally restart the app to pick up the change.

.DESCRIPTION
    The App Service container mounts an Azure Files share at /home/data and
    runs the application directly out of <share>/<RemoteAppDir> — there is no
    Docker image rebuild involved in a normal code update. This script:

      1. Fetches a short-lived SAS token for the file share at run time via
         `az storage account keys list` (requires `az login`; nothing
         sensitive is stored in this script or in source control).
      2. azcopy's the local working tree to the share, excluding VCS
         metadata, caches, and data directories that should not be
         overwritten by a code deploy.
      3. Optionally stops/starts the webapp so the new code takes effect.

    All environment-specific values (webapp name, resource group, storage
    account, share name, remote path) are parameters with defaults matching
    the current production deployment — override them for a different
    environment (e.g. a staging slot) instead of editing this file.

.PARAMETER LocalRepoPath
    Local working tree to upload. Default: this script's own directory.

.PARAMETER WebAppName
    Azure App Service name. Default: cdecmap.

.PARAMETER ResourceGroup
    Resource group containing the webapp and storage account.
    Default: dwrbdo_dash_rg.

.PARAMETER StorageAccountName
    Storage account backing the webapp's Azure Files mount.
    Default: dwrbdodashstore.

.PARAMETER FileShareName
    File share name. Default: data.

.PARAMETER RemoteAppDir
    Path within the share that the webapp is configured to run from.
    Default: cdec-maps (matches the App Service startup command
    `-d /home/data/cdec-maps`).

.PARAMETER ExcludePatterns
    Name patterns (wildcards allowed) never uploaded — data repositories,
    caches, and VCS metadata. Matched against top-level entries of
    LocalRepoPath (pruning the whole subtree) and, belt-and-suspenders,
    against nested file names too. Override if your working tree layout
    differs.

.PARAMETER RestartApp
    Stop then start the webapp after copying so the new code is loaded.
    Omit this to stage files without an outage (e.g. before a maintenance
    window), then run with -RestartApp -CopyFiles:$false separately.

.PARAMETER CopyFiles
    Copy files to the share. Defaults to $true; pass -CopyFiles:$false to
    only restart the app (e.g. after clearing a stale cache by hand).

.PARAMETER SasExpiryMinutes
    Lifetime of the generated SAS token used for the transfer.
    Default: 30 minutes.

.PARAMETER WhatIf
    Perform an azcopy dry run (no data transferred) so you can review what
    would change before actually deploying.

.EXAMPLE
    ./deploy_to_azure.ps1 -WhatIf
    ./deploy_to_azure.ps1 -RestartApp
    ./deploy_to_azure.ps1 -RemoteAppDir cdec-maps-staging -RestartApp:$false
#>
[CmdletBinding()]
param(
    [string]$LocalRepoPath = $PSScriptRoot,
    [string]$WebAppName = "cdecmap",
    [string]$ResourceGroup = "dwrbdo_dash_rg",
    [string]$StorageAccountName = "dwrbdodashstore",
    [string]$FileShareName = "data",
    [string]$RemoteAppDir = "cdec-maps",
    [string[]]$ExcludePatterns = @(
        ".git", "__pycache__", ".pytest*", "cache*",
        "cdec_db*", "*.egg-info", "junit.xml"
    ),
    [switch]$CopyFiles = $true,
    [switch]$RestartApp,
    [int]$SasExpiryMinutes = 30,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI ('az') not found on PATH. Install it or add its install directory to PATH."
}
if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw "azcopy not found on PATH. Install it from https://aka.ms/azcopy."
}

function New-ShareSasToken {
    Write-Host "==> Generating a short-lived SAS token (no secrets stored on disk)..." -ForegroundColor Cyan
    $expiry = (Get-Date).ToUniversalTime().AddMinutes($SasExpiryMinutes).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $sas = az storage share generate-sas `
        --account-name $StorageAccountName `
        --name $FileShareName `
        --permissions rcwl `
        --expiry $expiry `
        --https-only `
        --output tsv
    if (-not $sas) {
        throw "Failed to generate a SAS token. Confirm you are logged in ('az login') with access to storage account '$StorageAccountName'."
    }
    return "?$sas"
}

function Test-NameMatchesAnyPattern {
    param([string]$Name, [string[]]$Patterns)
    foreach ($p in $Patterns) { if ($Name -like $p) { return $true } }
    return $false
}

function Resolve-ExcludeRelativePaths {
    # azcopy's --exclude-pattern only filters leaf file names, so a
    # directory like "cdec_db" or "tests\__pycache__" is still traversed
    # and its contents uploaded. Resolve patterns against the real tree and
    # pass matches as --exclude-path, which prunes the whole subtree.
    param([string]$Root, [string[]]$Patterns)

    $rootFull = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\', '/')
    $resolved = [System.Collections.Generic.List[string]]::new()

    # Pass 1: top-level entries (cheap).
    $topLevelItems = Get-ChildItem -LiteralPath $rootFull -Force
    $topLevelMatches = $topLevelItems |
        Where-Object { Test-NameMatchesAnyPattern $_.Name $Patterns } |
        Select-Object -ExpandProperty Name
    $resolved.AddRange([string[]]$topLevelMatches)

    # Pass 2: nested matches anywhere else in the tree (e.g. tests\__pycache__).
    # Get-ChildItem's -Exclude only filters results, it does not stop
    # recursion, so recurse only into top-level directories that are not
    # already fully excluded — this keeps the scan out of .git and large
    # data directories like cdec_db/ entirely rather than just hiding
    # them from the output.
    $remainingDirs = $topLevelItems | Where-Object {
        $_.PSIsContainer -and ($topLevelMatches -notcontains $_.Name)
    }
    foreach ($dir in $remainingDirs) {
        Get-ChildItem -LiteralPath $dir.FullName -Recurse -Force -Directory -ErrorAction SilentlyContinue |
            Where-Object { Test-NameMatchesAnyPattern $_.Name $Patterns } |
            ForEach-Object {
                $rel = $_.FullName.Substring($rootFull.Length).TrimStart('\', '/') -replace '\\', '/'
                $resolved.Add($rel)
            }
    }

    return $resolved | Select-Object -Unique
}

function Copy-RepoFiles {
    param([string]$SasToken)

    $baseUrl = "https://$StorageAccountName.file.core.windows.net/$FileShareName/$RemoteAppDir"
    $env:AZCOPY_CRED_TYPE = "Anonymous"
    $env:AZCOPY_CONCURRENCY_VALUE = "AUTO"
    try {
        $excludePaths = Resolve-ExcludeRelativePaths -Root $LocalRepoPath -Patterns $ExcludePatterns
        $excludePathArg = ($excludePaths -join ";")
        $excludePatternArg = ($ExcludePatterns -join ";")

        Write-Host "==> Excluding: $excludePathArg" -ForegroundColor DarkGray

        $azcopyArgs = @(
            "copy", "$LocalRepoPath\*", "$baseUrl/$SasToken",
            "--exclude-path=$excludePathArg",
            "--exclude-pattern=$excludePatternArg",
            "--overwrite=true",
            "--check-length=true",
            "--put-md5",
            "--follow-symlinks",
            "--preserve-smb-info=true",
            "--recursive",
            "--log-level=INFO"
        )
        if ($WhatIf) { $azcopyArgs += "--dry-run" }

        Write-Host "==> Copying '$LocalRepoPath' -> share:$RemoteAppDir ..." -ForegroundColor Cyan
        & azcopy @azcopyArgs
        if ($LASTEXITCODE -ne 0) { throw "azcopy exited with code $LASTEXITCODE" }
        Write-Host "==> File copy complete." -ForegroundColor Green
    }
    finally {
        Remove-Item Env:\AZCOPY_CRED_TYPE, Env:\AZCOPY_CONCURRENCY_VALUE -ErrorAction SilentlyContinue
    }
}

function Restart-WebApp {
    Write-Host "==> Stopping webapp '$WebAppName'..." -ForegroundColor Cyan
    az webapp stop --name $WebAppName --resource-group $ResourceGroup | Out-Null
    Write-Host "==> Starting webapp '$WebAppName'..." -ForegroundColor Cyan
    az webapp start --name $WebAppName --resource-group $ResourceGroup | Out-Null
    Write-Host "==> Webapp restart complete." -ForegroundColor Green
}

if ($CopyFiles) {
    $sasToken = New-ShareSasToken
    Copy-RepoFiles -SasToken $sasToken
}
if ($RestartApp -and -not $WhatIf) {
    Restart-WebApp
}
elseif ($RestartApp -and $WhatIf) {
    Write-Host "==> Skipping webapp restart (-WhatIf)." -ForegroundColor Yellow
}
