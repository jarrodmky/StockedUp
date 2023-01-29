py -m pip install --upgrade pip virtualenv
py -m pip install -U mypy

#expect kivy source cloned
$kivy_source_path = "${PSScriptRoot}\..\kivy"
if(Test-Path $kivy_source_path)
{
    py -m virtualenv stockedup_venv
    stockedup_venv\Scripts\activate

    py -m pip install requests==2.28.1
    py -m pip install pandas==1.4.3
    py -m pip install matplotlib==3.6.2

    pushd $kivy_source_path
    py -m pip install -e ".[dev,base]"
    popd

    stockedup_venv\Scripts\deactivate
}