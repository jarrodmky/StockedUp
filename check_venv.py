import typing
from pathlib import Path
from sys import executable as python_exec
from virtualenv import cli_run as virtualenv_cli_run
from ensurepip import bootstrap as ensurepip_bootstrap
from subprocess import run

from Code.logger import get_logger
logger = get_logger(__name__)

def run_command_line(cmd_line_list : typing.List[str], working_directory : Path) -> bool :
	cmd_line_join = " ".join(cmd_line_list)
	logger.info(f"Running command \"{cmd_line_join}\" ...")

	successful = False
	try:
		rc = run(cmd_line_list, cwd=working_directory)

		if rc.returncode != 0 :
			logger.info(f"... command \"{cmd_line_join}\" ran with error!")
		else :
			logger.info(f"... command \"{cmd_line_join}\" successfully ran!")
			successful = True

	except Exception as e:
		logger.info(f"... command \"{cmd_line_join}\" call FAILED with exception: {e}")

	return successful

venv_script_path = Path("stockedup_venv/Scripts").absolute()

def run_py_module_command(module_command : str) -> bool :
    return run_command_line([python_exec, "-m"] + str.split(module_command, " "), Path.cwd())

def run_venvpy_module_command(module_command : str) -> bool :
    return run_command_line([str(venv_script_path.joinpath("python")), "-m"] + str.split(module_command, " "), Path.cwd())

def create_venv() :
    ensurepip_bootstrap(upgrade=True)

    if not run_py_module_command("pip install --upgrade pip virtualenv") :
        raise RuntimeError("Installing virtualenv failed!")

    #expects kivy source cloned to adjacent directory
    kivy_source_path = Path("../kivy").absolute()

    if not kivy_source_path.exists() or not kivy_source_path.is_dir() :
        raise FileNotFoundError(f"No Kivy source path found in absolute path {kivy_source_path}")

    session = virtualenv_cli_run(["--download", "--setuptools=bundle", "--wheel=bundle", "--copies", "stockedup_venv"])
    if session is not None :
        if run_venvpy_module_command("pip install -r dependencies.txt") :
            print("All installed suceeded!")
        else :
            print("Some package install failed!")

def check_venv() :
    venv_py_path = Path("stockedup_venv/Scripts/python.exe")

    if not venv_py_path.is_file():
        print(f'Virtual environment does not exist: "{venv_py_path}", creating...')
        
        create_venv()
    
    if not venv_py_path.is_file():
        raise FileNotFoundError(f'Cannot access "{venv_py_path}", setup failed!')

if __name__ == "__main__":
    check_venv()
