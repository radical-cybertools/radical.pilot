#!/bin/sh

# test -z "$RADICAL_DEBUG" || set -x

# avoid user aliases
unset -f printf kill true false trap mkdir

# be friendly to bash users
HISTIGNORE='*'
export HISTIGNORE


# This script implements process management capabilities for RP on the SHELL
# level, thus freeing the Python layer from this task.
#
# It functions as follows:
#
#  - the Python shell execution component creates a runnable shell script in the
#    unit sandbox (`unit.uid/unit.uid.sh`)
#  - the unit ID is sent to this script via a named pipe
#  - the script will listen on the named pipe, for three types of lines
#      EXEC unit.uid
#      KILL unit.uid
#      EXIT
#  - if a new ID is incoming (EXEC), it will run the respective script in the
#    background.  A pid-to-unit id map is stored on the file system, under
#    ($BASE/pids/[pid].uid and $BASE/pids/[uid].pid)
#  - on a KILL request, kill the respective process (if it was started).  
#    No guarantees are made on the kill - we just send SIGKILL and hope 
#    for the best
#  - the EXIT request will obviously call for an exit - running units will not
#    be killed.
#  - if this script dies or exits, it is the responsibility of the Python layer
#    to kill all remanining units - their PIDs can be found in the map dir.


# ------------------------------------------------------------------------------
#
usage(){

    ret=$1
    msg=$2

    if test -z "$msg"
    then
        printf "\n\t%s" "$msg"
    fi

    printf "\n\tusage: $0 <BASE> <SID>\n\n"

    exit $ret
}


# ------------------------------------------------------------------------------
#
error(){
    \printf "ERROR: %s\n" "$*"
    exit 1
}


# ------------------------------------------------------------------------------
#
log(){
    now=$(date "+%Y-%m-%d %H:%M:%S.%3N")
    \printf "$now $*\n"
}


# ------------------------------------------------------------------------------
#
prof(){
    test -z "$RP_GTOD" && return
    uid=$1
    evt=$2
    now=$($RP_GTOD)
    \printf "$now,$evt,'shell_spawner,MainThread,$uid,AGENT_EXECUTING,\n" \
        >> "$BASE/$uid/$uid.prof"
}

# ------------------------------------------------------------------------------
#
# set up
#
#   $1 - BASE: where to keep state for all tasks (defaults to pwd)
#              should be on a fast FS (eg. `/tmp/`)
#   $2 - SID : UID for this shell instance  (in case multiple instances coexist)
#
# setup() will respawn this script in irder to redirect all stdout and stderr 
# to $LOG - we ensure here that the respawn happened.
#
setup(){

    # respawn with redirection to log file
    if test -z "$_RESPAWNED"
    then
        # setup before respawn (only env settings, please)
        BASE="$1"
        SID="$2"
        LOG="$BASE/sh.$SID.log"
        
        test -z "$BASE" && usage 1 'missing base' 
        test -z "$SID"  && usage 1 'missing uid'
    
        export BASE
        export SID
        export LOG
    
        _RESPAWNED=$SID
        export _RESPAWNED
        exec > $LOG 2>&1 
    fi


    # remaining setup after respawn
    test "$_RESPAWNED" = "$SID" \
        || (\printf "respawn failure: [$_RESPAWNED] [$SID]\n";
            exit)

    log 'startup'

    PIPE_CMD="$BASE/$SID.cmd.pipe"
    PIPE_INF="$BASE/$SID.inf.pipe"
    MAP="$BASE/sh.$SID.pids"

    test -e "$PIPE_CMD" || error "missing input  pipe $PIP_IN"
    test -e "$PIPE_INF" || error "missing output pipe $PIP_OUT"
    test -f "$LOG"      || error "missing logfile $LOG"

    # create location to manage pid to unit.uid maps
    \mkdir -p "$MAP" || error "cannot create mapdir"
    
}


# ------------------------------------------------------------------------------
#
\trap cleanup_handler QUIT
\trap cleanup_handler TERM
\trap cleanup_handler EXIT
\trap cleanup_handler HUP
\trap cleanup_handler INT
\trap cleanup_handler TERM

cleanup_handler(){

    log INFO "cleanup"
}


# ------------------------------------------------------------------------------
#
do_exec(){

    uid="$1"
    exe="$BASE/$uid/$uid.sh"
    out="$BASE/$uid/STDOUT"
    err="$BASE/$uid/STDERR"

    log INFO "exec $uid"

    prof "$uid" 'pre_spawn'
    (
        (set -m
            /bin/sh "$exe" 1>"$out" 2>"$err"
        ) 1>/dev/null 2>/dev/null 3</dev/null &

        pid=$!
        prof "$uid" 'post_spawn'

        \printf "$pid\n" > $MAP/$uid.pid
        \printf "$uid\n" > $MAP/$pid.uid
        prof "$uid" 'post_record'
        exit
    )
}


# ------------------------------------------------------------------------------
#
do_kill(){

    uid="$1"
    log INFO "kill $uid"

    prof "$uid" 'pre_kill'
    \kill -9 $(cat $BASE/pids/$uid.pid) || \true  # ignore failures
    prof "$uid" 'post_kill'
}


# ------------------------------------------------------------------------------
#
do_exit(){

    ret=$1; shift
    log INFO "exit requested [$*]"
    exit $ret
}


# ------------------------------------------------------------------------------
# listen for requests, and serve them
work(){

    while \true
    do
        cmd=''
        id=''
        read -r cmd id < $PIPE_CMD || do_exit 1 'read failed'

        case "$cmd" in 
            EXEC) do_exec   "$id";;
            KILL) do_kill   "$id";;
            EXIT) do_exit 0 "$id";;
            *   ) log FAIL "cannot handle [$cmd] [$id]";;
        esac
    done
}


# ------------------------------------------------------------------------------
# main
setup "$@"
work

# ------------------------------------------------------------------------------

