#!/bin/bash

stocked_up_path="./stocked_up.py"
if [[ ! -f "$stocked_up_path" ]]; then
    echo "Can't access \"$stocked_up_path\", any repository changes?"
    exit 1
fi

venv_py_path="stockedup_venv/Scripts/python.exe"
if [[ ! -f "$venv_py_path" ]]; then
    echo "Virtual environment does not exist: \"$venv_py_path\", creating..."
    create_venv_path="create_venv.py"
    if [[ ! -f "$create_venv_path" ]]; then
        echo "Can't access \"$create_venv_path\", any repository changes?"
        exit 1
    fi
    python "$create_venv_path"
fi

if [[ ! -f "$venv_py_path" ]]; then
    echo "Can't access \"$venv_py_path\", setup failed!"
    exit 1
fi

export KIVY_LOG_MODE="PYTHON"
export PYTHON="$venv_py_path"
export VENV_DIR="./stockedup_venv"
export GIT=""

"$PYTHON" -B "$stocked_up_path" -- --data_directory ./Data "$@"