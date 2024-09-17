$local:stocked_up_path = "./stocked_up.py"
if (!(Test-Path $stocked_up_path -PathType Leaf))
{
    throw "Can't access ""$stocked_up_path"", any reposity changes?"
}

$local:venv_py_path = "stockedup_venv\Scripts\python.exe"
if (!(Test-Path $venv_py_path -PathType Leaf))
{
    Write-Host "Virtual environment does not exist: ""$venv_py_path"", creating..."
    $local:create_venv_path = "create_venv.py"
    if (!(Test-Path $create_venv_path -PathType Leaf))
    {
        throw "Can't access ""$create_venv_path"", any reposity changes?"
    }
    & py $create_venv_path
}
if (!(Test-Path $venv_py_path -PathType Leaf))
{
    throw "Can't access ""$venv_py_path"", setup failed!"
}

$env:KIVY_LOG_MODE = "PYTHON"
$env:PYTHON = $venv_py_path
$env:VENV_DIR="./stockedup_venv"
$env:GIT=

& $env:PYTHON -B $stocked_up_path -- --data_directory ./Data $args
