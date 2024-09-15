from os import path as os_path
from os import name as os_name
from mypy import api as mypy_api

def get_null_device_path() :
    null_device = ""
    
    # Check if the operating system is Linux or Windows
    if os_name == "posix":  # Linux
        null_device = "/dev/null"
    elif os_name == "nt":   # Windows
        null_device = "nul" if os_path.exists("nul") else "NUL"
    else:
        raise OSError("Unsupported operating system")

    return null_device

def run_type_check() :
    try :
        result = mypy_api.run(["Code"
                      , "--disallow-incomplete-defs"
                      , "--no-incremental"
                      , "--check-untyped-defs"
                      , f"--cache-dir={get_null_device_path()}"
                      , "--ignore-missing-import"])

        if result[0] :
            print('\nType checking report:\n')
            print(result[0])  # stdout

        if result[1] :
            print('\nError report:\n')
            print(result[1])  # stderr
    except Exception as e :
        print(f"\nException hit during type check : {e}")

    return result[2]