#!/bin/bash

# Define paths
stocked_up_path="./stocked_up.py"
check_venv_path="check_venv.py"

# Check if stocked_up.py exists
if [[ ! -f "$stocked_up_path" ]]; then
    echo "Can't access \"$stocked_up_path\", any repository changes?"
    exit 1
fi

# Check if check_venv.py exists
if [[ ! -f "$check_venv_path" ]]; then
    echo "Can't access \"$check_venv_path\", any repository changes?"
    exit 1
fi

# Run check_venv.py
python "$check_venv_path"

# Set environment variables
export KIVY_LOG_MODE="PYTHON"
export PYTHON="stockedup_venv/bin/python"
export VENV_DIR="./stockedup_venv"

# Run stocked_up.py with arguments
"$PYTHON" -B "$stocked_up_path" -- --data_directory ./Data "$@"