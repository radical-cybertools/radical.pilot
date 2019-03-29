#!/bin/sh


# Environment variables
export RP_SESSION_ID="rp.session.rivendell.merzky.017756.0000"
export RP_PILOT_ID="pilot.0000"
export RP_AGENT_ID="agent_0"
export RP_SPAWNER_ID="agent_0.executing.0.child"
export RP_UNIT_ID="unit.000000"
export RP_GTOD="/home/merzky/radical.pilot.sandbox/rp.session.rivendell.merzky.017756.0000/pilot.0000/gtod"
export RP_TMP="None"
unset  RP_PROF

prof(){
    if test -z "$RP_PROF"
    then
        return
    fi
    event=$1
    now=$($RP_GTOD)
    echo "$now,$event,unit_script,MainThread,$RP_UNIT_ID,AGENT_EXECUTING," >> $RP_PROF
}
export OMP_NUM_THREADS="1"

prof cu_start

# Change to unit sandbox
cd /home/merzky/radical.pilot.sandbox/rp.session.rivendell.merzky.017756.0000/pilot.0000/unit.000000
prof cu_cd_done

# The command to run
prof cu_exec_start
/home/merzky/radical/ompi/installed/2017_09_18_539f71d/bin//orterun  --ompi-server "2365652992.0;tcp://127.0.0.1:53981" -np 1 --bind-to none -host localhost -x "LD_LIBRARY_PATH" -x "PATH" -x "PYTHONPATH"  /bin/true
RETVAL=$?
prof cu_exec_stop

# Exit the script with the return code from the command
prof cu_stop
exit $RETVAL
