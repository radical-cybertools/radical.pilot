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

while getopts "o:" OPTION; do
    case $OPTION in
        o) OUTPUT_DIR="$OPTARG" ;;
        *) exit 1               ;;
    esac
done

base_url="https://raw.githubusercontent.com/radical-cybertools/radical.pilot/v$rp_version/examples"
wget -q "$base_url/config.json"
wget -q "$base_url/00_getting_started.py"
chmod +x 00_getting_started.py

radical-stack
RADICAL_REPORT_ANIME=FALSE ./00_getting_started.py local.localhost
exitcode=$?

# collect session sandboxes as tarballs
session_id=$(find . -maxdepth 1 -name "rp.session*" -type d -exec basename {} "rp.session" \;)
tar -czf session_client.tar.gz `find $session_id -type d -print`
session_agent_sbox="$HOME/radical.pilot.sandbox/$session_id"
if [ -d "$session_agent_sbox" ]; then
  tar -P -czf session_agent.tar.gz $session_agent_sbox
fi
mkdir -p $OUTPUT_DIR
mv session_*.tar.gz $OUTPUT_DIR/
echo "sid           : $session_id"
echo "agent sandbox : $session_agent_sbox"

test "$exitcode" = 0 && echo "Success!"
exit $exitcode

