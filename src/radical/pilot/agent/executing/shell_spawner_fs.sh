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
#    task sandbox (`task.uid/task.uid.sh`)
#  - the task ID is sent to this script via a named pipe
#  - the script will listen on the named pipe, for three types of lines
#      EXEC task.uid
#      KILL task.uid
#      EXIT
#  - if a new ID is incoming (EXEC), it will run the respective script in the
#    background.  A pid-to-task id map is stored on the file system, under
#    ($WORK/pids/[pid].uid and $WORK/pids/[uid].pid)
#  - on a KILL request, kill the respective process (if it was started).
#    No guarantees are made on the kill - we just send SIGKILL and hope
#    for the best
#  - the EXIT request will obviously call for an exit - running tasks will not
#    be killed.
#  - if this script dies or exits, it is the responsibility of the Python layer
#    to kill all remanining tasks - their PIDs can be found in the map dir.


# ------------------------------------------------------------------------------
#
usage(){

    ret=$1
    msg=$2

    if test -z "$msg"
    then
        printf "\n\t%s" "$msg"
    fi

    printf "\n\tusage: $0 <BASE> <WORK> <SID>\n\n"

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
  # test -z "$RP_GTOD" && return
    uid=$1
    evt=$2
  # now=$($RP_GTOD)
    now=$($BASE/gtod)
    \printf "$now,$evt,'shell_spawner,MainThread,$uid,AGENT_EXECUTING,\n" \
        >> "$BASE/$uid/$uid.prof"
}

# ------------------------------------------------------------------------------
#
# set up
#
#   $1 - BASE: where the task sandboxes are kept
#   $2 - WORK: where to keep state for all tasks and fifo's
#              should be on a fast FS (eg. `/tmp/`)
#   $3 - SID : SID for this shell instance  (in case multiple instances coexist)
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
        WORK="$2"
        SID="$3"
        LOG="$WORK/sh.$SID.log"

        test -z "$BASE" && usage 1 'missing base dir'
        test -z "$WORK" && usage 1 'missing work dir'
        test -z "$SID"  && usage 1 'missing sid'

        export BASE
        export WORK
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

    PIPE_CMD="$WORK/$SID.cmd.pipe"
    PIPE_INF="$WORK/$SID.inf.pipe"
    MAP="$WORK/sh.$SID.pids"
    export MAP

    test -e "$PIPE_CMD" || error "missing input  pipe $PIP_IN"
    test -e "$PIPE_INF" || error "missing output pipe $PIP_OUT"
    test -f "$LOG"      || error "missing logfile $LOG"

    # create location to manage pid to task.uid maps
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
    out="$BASE/$uid/$uid.out"
    err="$BASE/$uid/$uid.err"

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
    \kill -9 $(cat $MAP/$uid.pid) || \true  # ignore failures
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
      # log DEBUG "read"
        read -r cmd id < $PIPE_CMD || do_exit 1 'read failed'
      # log DEBUG "read: [$cmd $id]"

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

