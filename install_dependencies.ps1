py -m pip install --upgrade pip mypy virtualenv

#expect kivy source cloned
$kivy_source_path = "${PSScriptRoot}\..\kivy"
if(Test-Path $kivy_source_path)
{
    py -m virtualenv stockedup_venv
    stockedup_venv\Scripts\activate

    py -m pip install requests==2.28.1
    py -m pip install pandas==2.1.1
    py -m pip install matplotlib==3.8.0

    pushd $kivy_source_path
    py -m pip install -e ".[dev,base]"
    popd

    stockedup_venv\Scripts\deactivate
}