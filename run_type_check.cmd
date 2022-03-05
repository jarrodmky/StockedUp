@echo off

py -m mypy Code/json_file.py
py -m mypy Code/utf8_file.py
py -m mypy Code/debug.py
py -m mypy Code/accounting_objects.py
py -m mypy Code/accounting.py
py -m mypy Code/csv_importing.py
py -m mypy Code/stocked_up.py
py -m mypy Code/import_csv_data.py
py -m mypy Code/map_spending_accounts.py

rmdir /s /q .mypy_cache
