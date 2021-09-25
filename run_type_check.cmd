@echo off

mypy Code/json_file.py
mypy Code/utf8_file.py
mypy Code/debug.py
mypy Code/accounting_objects.py
mypy Code/accounting.py
mypy Code/csv_importing.py
mypy Code/stocked_up.py
mypy Code/import_csv_data.py
mypy Code/map_spending_accounts.py

rmdir /s /q .mypy_cache
