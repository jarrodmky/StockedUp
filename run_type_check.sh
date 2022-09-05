#!/bin/sh
python3.10 -m mypy Code --disallow-incomplete-defs --no-incremental --cache-dir=/dev/null --ignore-missing-import
