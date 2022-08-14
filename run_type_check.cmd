@echo off

py -m mypy Code --disallow-incomplete-defs --no-incremental --cache-dir=nul --ignore-missing-import
