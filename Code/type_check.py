from sys import executable as py_exec
from os import path as check_path
from os import name as os_name
import pathlib

from PyJMy.subprocess_handling import run_command_line

def get_null_device_path() :
    null_device = ""
    
    # Check if the operating system is Linux or Windows
    if os_name == "posix":  # Linux
        null_device = "/dev/null"
    elif os_name == "nt":   # Windows
        null_device = "nul" if check_path.exists("nul") else "NUL"
    else:
        raise OSError("Unsupported operating system")

    return null_device

def run_type_check() :
    return run_command_line([py_exec
                      , "-m", "mypy", "Code"
                      , "--disallow-incomplete-defs"
                      , "--no-incremental"
                      , "--check-untyped-defs"
                      , f"--cache-dir={get_null_device_path()}"
                      , "--ignore-missing-import"
                      ]
                      , pathlib.Path.cwd())