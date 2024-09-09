import typing
from pathlib import Path
from sys import executable as python_exec
from virtualenv import cli_run as virtualenv_cli_run
from Code.PyJMy.subprocess_handling import run_command_line

venv_script_path = Path("stockedup_venv/Scripts").absolute()

def run_py_module_command(module_command : str) -> bool :
    return run_command_line([python_exec, "-m"] + str.split(module_command, " "), Path.cwd())

def run_venvpy_module_command(module_command : str) -> bool :
    return run_command_line([str(venv_script_path.joinpath("python")), "-m"] + str.split(module_command, " "), Path.cwd())

if not run_py_module_command("ensurepip") :
    raise RuntimeError("Installing pip failed!")

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
