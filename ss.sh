#!/usr/bin/env bash

CURRENT_PATH=$(
cd $(dirname "${BASH_SOURCE[0]}")
pwd
)
cd ${CURRENT_PATH}

python3 ./scheduler.py $*
