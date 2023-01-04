@echo off

py -m mypy Code --disallow-incomplete-defs --no-incremental --check-untyped-defs --cache-dir=nul --ignore-missing-import
