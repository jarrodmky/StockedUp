#!/bin/sh
python3.10 -m ensurepip
python3.10 -m pip install --upgrade pip
python3.10 -m pip install -U mypy
python3.10 -m pip install requests==2.28.1
python3.10 -m pip install pandas==1.4.3
python3.10 -m pip install matplotlib==3.6.2
python3.10 -m pip install "kivy[base]"==2.1.0
