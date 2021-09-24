@echo off
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/json_file.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/utf8_file.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/debug.py

%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/accounting_objects.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/accounting.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/csv_importing.py

%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/stocked_up.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/import_csv_data.py
%appdata%\..\Local\Programs\Python\Python38\Scripts\mypy Code/map_spending_accounts.py

rmdir /s /q .mypy_cache
