import typing
from pathlib import Path
from sys import executable as python_exec
from Code.PyJMy.subprocess_handling import run_command_line

venv_script_path = Path("stockedup_venv/Scripts").absolute()
venv_py_exec = venv_script_path.joinpath("python")
activate_script = venv_script_path.joinpath("activate_this.py")

def run_py_module_command(module_command : str) -> bool :
    return run_command_line([python_exec, "-m"] + str.split(module_command, " "), Path.cwd())

def run_venvpy_module_command(module_command : str) -> bool :
    return run_command_line([str(venv_py_exec), "-m"] + str.split(module_command, " "), Path.cwd())

def run_batch_file(batch_filename : str) -> bool :
    return run_command_line([str(venv_script_path.joinpath(batch_filename))], Path.cwd())

if not run_py_module_command("pip install --upgrade pip") :
    raise RuntimeError("Installing pip or mypy or virtualenv failed!")

#expects kivy source cloned to adjacent directory
kivy_source_path = Path("../kivy").absolute()

if not kivy_source_path.exists() or not kivy_source_path.is_dir() :
    raise FileNotFoundError(f"No Kivy source path found in absolute path {kivy_source_path}")

if run_py_module_command("venv stockedup_venv") :

    if run_batch_file("activate.bat") :

        if run_venvpy_module_command("pip install -r dependencies.txt") :
           print("All installed suceeded!")
        else :
           print("Some package install failed!")

        run_batch_file("deactivate.bat")
