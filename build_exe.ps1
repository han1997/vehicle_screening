param(
    [switch]$SkipDepsInstall,
    [switch]$Win7Compatible,
    [string]$PythonExe
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $projectRoot

if (-not $PythonExe) {
    $PythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
}

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python executable not found: $PythonExe. Create a venv first, or pass -PythonExe explicitly."
}

$pythonVersionText = & $PythonExe -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to read Python version from: $PythonExe"
}

$pythonVersion = [version]$pythonVersionText
$pyInstallerPackage = "pyinstaller"

if ($Win7Compatible) {
    if ($pythonVersion.Major -ne 3 -or $pythonVersion.Minor -ne 8) {
        throw "Win7 compatible build requires Python 3.8.x, current version is $pythonVersionText. Use -PythonExe to point to a Python 3.8 venv."
    }
    $pyInstallerPackage = "pyinstaller==5.13.2"
    Write-Host "Win7 compatible mode enabled (Python $pythonVersionText + $pyInstallerPackage)." -ForegroundColor Yellow
} elseif ($pythonVersion.Major -gt 3 -or ($pythonVersion.Major -eq 3 -and $pythonVersion.Minor -ge 9)) {
    Write-Warning "Python $pythonVersionText is not compatible with Windows 7. If target machine is Win7, rebuild with -Win7Compatible and Python 3.8."
}

if (-not $SkipDepsInstall) {
    & $PythonExe -m pip install --upgrade $pyInstallerPackage
    if ($LASTEXITCODE -ne 0) {
        throw "Dependency install failed. pip exit code: $LASTEXITCODE"
    }
}

$separator = ";"
$pyInstallerArgs = @(
    "--noconfirm"
    "--clean"
    "--onefile"
    "--runtime-tmpdir"
    ".\\_pyi_runtime"
    "--name"
    "vehicle_screening"
    "--add-data"
    "templates${separator}templates"
    "--add-data"
    "static${separator}static"
    "--add-data"
    "checkpoint_library.json${separator}."
    "--add-data"
    "keyperson_library.json${separator}."
    "app.py"
)
& $PythonExe -m PyInstaller @pyInstallerArgs

if ($LASTEXITCODE -ne 0) {
    throw "Build failed. PyInstaller exit code: $LASTEXITCODE"
}

Write-Host "Build completed: dist\\vehicle_screening.exe"
