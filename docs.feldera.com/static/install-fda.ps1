# Feldera fda CLI installer for Windows
# Usage:
#   irm https://docs.feldera.com/install-fda.ps1 | iex
#   $env:FDA_VERSION="v0.290.0"; irm https://docs.feldera.com/install-fda.ps1 | iex
#   $env:FELDERA_INSTALL="C:\tools\feldera"; irm https://docs.feldera.com/install-fda.ps1 | iex

$ErrorActionPreference = 'Stop'

$FelderaInstall = if ($env:FELDERA_INSTALL) { $env:FELDERA_INSTALL } else { "$env:USERPROFILE\.feldera" }
$BinDir = "$FelderaInstall\bin"

# AddressWidth is OS bitness, not CPU arch: blocks 32-bit Windows only.
# arm64 passes and gets the x86_64 binary, which runs via Windows emulation.
$Arch = (Get-CimInstance Win32_Processor).AddressWidth
if ($Arch -ne 64) {
    Write-Error "Unsupported OS: only 64-bit Windows is supported."
    exit 1
}
$Target = "x86_64-pc-windows-msvc"

if ($env:FDA_VERSION) {
    $Version = $env:FDA_VERSION
    $DownloadUrl = "https://github.com/feldera/feldera/releases/download/$Version/fda-$Target.zip"
    Write-Host "Installing fda $Version"
} else {
    $DownloadUrl = "https://github.com/feldera/feldera/releases/latest/download/fda-$Target.zip"
    Write-Host "Installing fda (latest)"
}

$TmpDir = [System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString()
New-Item -ItemType Directory -Path $TmpDir | Out-Null

try {
    $TmpZip = "$TmpDir\fda.zip"

    Write-Host "Downloading from $DownloadUrl"
    Invoke-WebRequest -Uri $DownloadUrl -OutFile $TmpZip -UseBasicParsing

    Write-Host "Extracting fda binary"
    Expand-Archive -Path $TmpZip -DestinationPath $TmpDir -Force

    New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
    Move-Item -Force "$TmpDir\fda.exe" "$BinDir\fda.exe"

    Write-Host "Installed fda to $BinDir\fda.exe"
} finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# Add to PATH (user scope) if not already present
$UserPath = [Environment]::GetEnvironmentVariable('PATH', 'User')
if ($UserPath -notlike "*$BinDir*") {
    [Environment]::SetEnvironmentVariable('PATH', "$BinDir;$UserPath", 'User')
    Write-Host "Added $BinDir to PATH (user). Restart your shell for it to take effect."
}

Write-Host ""
Write-Host "fda was installed successfully!"
& "$BinDir\fda.exe" --version 2>$null

Write-Host ""
Write-Host "To enable shell completions, see:"
Write-Host "  https://docs.feldera.com/docs/interface/cli#optional-shell-completion"
