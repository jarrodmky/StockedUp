from os import path as os_path
from os import name as os_name
from mypy import api as mypy_api

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

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
        (normal_report, error_report, exit_status) = mypy_api.run(["Code"
                      , "--disallow-incomplete-defs"
                      , "--no-incremental"
                      , "--check-untyped-defs"
                      , f"--cache-dir={get_null_device_path()}"
                      , "--ignore-missing-import"])

        if normal_report and normal_report != "" :
            logger.info(f"Type checking report:\n{normal_report}")

        if error_report and error_report != "" :
            logger.warning(f"Type checking errors:\n{error_report}")
    except Exception as e :
        logger.exception(f"Exception hit during type check : {e}")

    return exit_status == 0