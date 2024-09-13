$local:stocked_up_path = "Code/stocked_up.py"
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

& $venv_py_path -B Code/stocked_up.py -- --data_directory ./Data $args
