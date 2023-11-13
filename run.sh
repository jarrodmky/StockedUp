#!/bin/sh
venv_py_path="stockedup_venv/Scripts/python.exe"
stocked_up_path="Code/stocked_up.py"
default_data_path="./Data"
create_venv_path="create_venv.py"

if [ ! -f "$stocked_up_path" ]; then
    echo "Can't access \"$stocked_up_path\", any repository changes?"
    exit 1
fi

if [ ! -f "$venv_py_path" ]; then
    echo "Virtual environment does not exist: \"$venv_py_path\", creating..."
    create_venv_path="create_venv.py"
    
    if [ ! -f "$create_venv_path" ]; then
        echo "Can't access \"$create_venv_path\", any repository changes?"
        exit 1
    fi
    
    python3 "$create_venv_path"
fi

if [ ! -f "$venv_py_path" ]; then
    echo "Can't access \"$venv_py_path\", setup failed!"
    exit 1
fi

"$venv_py_path" Code/stocked_up.py -- --data_directory ./Data "$@"