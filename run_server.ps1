$local:server_path = "./run_server.py"
if (!(Test-Path $server_path -PathType Leaf))
{
    throw "Can't access ""$server_path"", any reposity changes?"
}
$local:check_venv_path = "check_venv.py"
if (!(Test-Path $check_venv_path -PathType Leaf))
{
    throw "Can't access ""$check_venv_path"", any reposity changes?"
}

& py $check_venv_path

$env:PYTHON = "stockedup_venv/Scripts/python.exe"
$env:VENV_DIR="./stockedup_venv"

try
{
    & $env:VENV_DIR\Scripts\activate
    $subprocess = Start-Process -FilePath "prefect" -ArgumentList "server start" -NoNewWindow -PassThru

    & $env:PYTHON -OO $server_path $args
}
catch
{
    Write-Error "An error occurred: $_"
}
finally
{
    if ($null -ne $subprocess) {
        Stop-Process -Id $subprocess.Id -Force
    }
    & deactivate
}
