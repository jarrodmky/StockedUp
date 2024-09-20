$local:stocked_up_path = "./stocked_up.py"
if (!(Test-Path $stocked_up_path -PathType Leaf))
{
    throw "Can't access ""$stocked_up_path"", any reposity changes?"
}
$local:check_venv_path = "check_venv.py"
if (!(Test-Path $check_venv_path -PathType Leaf))
{
    throw "Can't access ""$check_venv_path"", any reposity changes?"
}

& py $check_venv_path

$env:KIVY_LOG_MODE = "PYTHON"
$env:PYTHON = "stockedup_venv/Scripts/python.exe"
$env:VENV_DIR="./stockedup_venv"

& $env:PYTHON -B $stocked_up_path -- --data_directory ./Data $args
