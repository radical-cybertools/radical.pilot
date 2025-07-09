#!/bin/bash

if which radical-pilot-version >/dev/null; then
    rp_version="$(radical-pilot-version)"
    if [[ -z $rp_version ]]; then
        echo "RADICAL-Pilot version unknown"
        exit 1
    fi
else
    echo "RADICAL-Pilot not installed"
    exit 1
fi

base_url="https://raw.githubusercontent.com/radical-cybertools/radical.pilot/v$rp_version/examples"
wget -q "$base_url/config.json"
wget -q "$base_url/00_getting_started.py"
chmod +x 00_getting_started.py

radical-stack
RADICAL_REPORT_ANIME=FALSE ./00_getting_started.py local.localhost
exitcode=$?

test "$exitcode" = 0 && echo "Success!"
exit $exitcode

