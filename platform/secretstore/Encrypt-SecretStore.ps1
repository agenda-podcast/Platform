# Encrypt-SecretStore.ps1
# Symmetric-encrypt a JSON file (same folder) into a .gpg file using GnuPG (gpg).
#
# Where to set passphrase:
#   Option A (recommended): set environment variable before running:
#       $env:SECRETSTORE_PASSPHRASE = "your-strong-passphrase"
#   Option B: if env var is not set, script will prompt (input hidden).
#
# This script does NOT delete the original JSON (per your request).

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---- Configuration (optional) ----
# If empty, script will use "secretstore.json" in this folder (or the first *.json excluding *.template.json)
$InputJsonName = ""   # e.g. "secretstore.json"
# If empty, script will output "<input>.gpg"
$OutputGpgName = ""   # e.g. "secretstore.json.gpg"
# Passphrase env var name
$PassphraseEnvVar = "SECRETSTORE_PASSPHRASE"
# ---- End configuration ----

function Get-ScriptDir {
  if ($PSScriptRoot) { return $PSScriptRoot }
  return (Split-Path -Parent $MyInvocation.MyCommand.Path)
}

function Ensure-Gpg {
  $gpg = Get-Command gpg -ErrorAction SilentlyContinue
  if (-not $gpg) {
    throw "gpg.exe not found on PATH. Install GnuPG and ensure 'gpg' is available in PATH."
  }
}

function Get-PassphrasePlain([string]$EnvVarName) {
  $fromEnv = [Environment]::GetEnvironmentVariable($EnvVarName)
  if ($fromEnv -and $fromEnv.Trim().Length -gt 0) {
    return $fromEnv
  }

  $secure = Read-Host "Enter passphrase for encryption (will not echo)" -AsSecureString
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
  }
}

function Resolve-InputJson([string]$Dir, [string]$InputName) {
  if ($InputName -and $InputName.Trim().Length -gt 0) {
    $p = Join-Path $Dir $InputName
    if (-not (Test-Path $p -PathType Leaf)) { throw "Input JSON not found: $p" }
    return (Resolve-Path $p).Path
  }

  $default = Join-Path $Dir "secretstore.json"
  if (Test-Path $default -PathType Leaf) {
    return (Resolve-Path $default).Path
  }

  $candidates = Get-ChildItem -Path $Dir -File -Filter "*.json" |
    Where-Object { $_.Name -notmatch '\.template\.json$' } |
    Sort-Object Name

  if (-not $candidates -or $candidates.Count -eq 0) {
    throw "No JSON found in $Dir (excluding *.template.json). Set `$InputJsonName."
  }

  return $candidates[0].FullName
}

function Resolve-OutputGpg([string]$Dir, [string]$InputPath, [string]$OutputName) {
  if ($OutputName -and $OutputName.Trim().Length -gt 0) {
    return (Join-Path $Dir $OutputName)
  }
  return "$InputPath.gpg"
}

# ---- Main ----
Ensure-Gpg
$dir = Get-ScriptDir

$inPath  = Resolve-InputJson -Dir $dir -InputName $InputJsonName
$outPath = Resolve-OutputGpg -Dir $dir -InputPath $inPath -OutputName $OutputGpgName

$pass = Get-PassphrasePlain -EnvVarName $PassphraseEnvVar
if (-not $pass -or $pass.Length -lt 12) {
  throw "Passphrase missing or too short. Use a strong passphrase (recommended 16+ chars)."
}

Write-Host "Encrypting:"
Write-Host "  Input : $inPath"
Write-Host "  Output: $outPath"
Write-Host "  Passphrase source: " -NoNewline
if ([Environment]::GetEnvironmentVariable($PassphraseEnvVar)) { Write-Host "env:$PassphraseEnvVar" } else { Write-Host "prompt" }

$psiArgs = @(
  "--batch",
  "--yes",
  "--pinentry-mode", "loopback",
  "--passphrase-fd", "0",
  "--symmetric",
  "--cipher-algo", "AES256",
  "--output", $outPath,
  $inPath
)

$procInfo = New-Object System.Diagnostics.ProcessStartInfo
$procInfo.FileName = "gpg"
$procInfo.Arguments = ($psiArgs -join " ")
$procInfo.RedirectStandardInput  = $true
$procInfo.RedirectStandardOutput = $true
$procInfo.RedirectStandardError  = $true
$procInfo.UseShellExecute = $false
$procInfo.CreateNoWindow  = $true

$proc = New-Object System.Diagnostics.Process
$proc.StartInfo = $procInfo
$null = $proc.Start()

$proc.StandardInput.WriteLine($pass)
$proc.StandardInput.Close()
$pass = $null

$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()

$proc.WaitForExit()
if ($proc.ExitCode -ne 0) {
  throw "gpg failed with exit code $($proc.ExitCode). Error: $stderr"
}

if (-not (Test-Path $outPath -PathType Leaf)) {
  throw "Encryption reported success but output file not found: $outPath"
}

Write-Host "Success: Encrypted file created at $outPath"
