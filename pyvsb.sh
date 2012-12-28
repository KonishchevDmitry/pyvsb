#!/bin/sh
#
# Runs pyvsb from the source code
#

PYTHONPATH="$PYTHONPATH:." python3 -c '__import__("pyvsb.main").main.main()' "$@"
