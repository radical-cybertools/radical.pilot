#!/bin/bash -l

# keep the original env so that we can use them layter for CU env settings.
env > env.orig

# before we change anything in the pilot environment, we safe a couple of
# env vars to later re-create a close-to-pristine env for unit execution.
_OLD_PYTHONPATH="$PYTHONPATH"
_OLD_PYTHONHOME="$PYTHONHOME"
_OLD_PATH="$PATH"
_OLD_PS1="$PS1"

export _OLD_PYTHONPATH
export _OLD_PYTHONHOME
export _OLD_PATH
export _OLD_PS1

# Unset functions/aliases of commands that will be used during bootsrap as
# these custom functions can break assumed/expected behavior.  Avoid anything to
# be called during prompts
export PS1='#'
export PS2='>'
export PS3='?'
export PS4='+'
unset PROMPT_COMMAND
unset -f cd ls uname pwd date bc cat echo


# interleave stdout and stderr, to get a coherent set of log messages
if test -z "$_RP_BOOTSTRAP_0_REDIR"
then
    echo "bootstrap_0 stderr redirected to stdout"
    export _RP_BOOTSTRAP_0_REDIR=True
    exec 2>&1
fi

if test "`uname`" = 'Darwin'
then
    echo 'Darwin: increasing open file limit'
    ulimit -n 512
fi

# ------------------------------------------------------------------------------
# Copyright 2013-2015, RADICAL @ Rutgers
# Licensed under the MIT License
#
# This script launches a radical.pilot compute pilot.  It expects a functional
# virtualenv to be available.
#
# https://xkcd.com/1987/
#
# Arguments passed to bootstrap_0 should be required by bootstrap_0 itself,
# and *not* be passed down to the agent.  Configuration used by the agent should
# go in the agent config file.
#
# ------------------------------------------------------------------------------
# global variables
#
TUNNEL_BIND_DEVICE="lo"
HOSTPORT=
CLEANUP=
VIRTENV=
CCM=
PILOT_ID=
PYTHON=
PYTHON_DIST=
SESSION_ID=
SESSION_SANDBOX=
PILOT_SANDBOX=`pwd`
PREBOOTSTRAP2=""

# NOTE:  $HOME is set to the job sandbox on OSG.  Bah!
# FIXME: the need for this needs to be reconfirmed and documented
# mkdir -p .ssh/


# ------------------------------------------------------------------------------
#
# disable user site packages as those can conflict with our virtualenv
# installation -- see https://github.com/conda/conda/issues/448
#
# NOTE: we need to make sure this is inherited into sub-agent shells
#
export PYTHONNOUSERSITE=True


# ------------------------------------------------------------------------------
#
profile_event()
{
    # FIXME: provide an RU shell script to source for this
    PROFILE="bootstrap_0.prof"

    if test -z "$RADICAL_PILOT_PROFILE$RADICAL_PROFILE"
    then
        return
    fi

    event=$1
    msg=$2

    NOW=`echo $(radical-utils-gtod) - "$TIME_ZERO" | bc`

    if ! test -f "$PROFILE"
    then
        # initialize profile
        echo "#time,name,uid,state,event,msg" > "$PROFILE"
    fi

    # TIME   = 0  # time of event (float, seconds since epoch)  mandatory
    # EVENT  = 1  # event ID (string)                           mandatory
    # COMP   = 2  # component which recorded the event          mandatory
    # TID    = 3  # uid of thread involved                      optional
    # UID    = 4  # uid of entity involved                      optional
    # STATE  = 5  # state of entity involved                    optional
    # MSG    = 6  # message describing the event                optional
    # ENTITY = 7  # type of entity involved                     optional
    printf "%.4f,%s,%s,%s,%s,%s,%s\n" \
        "$NOW" "$event" "bootstrap_0" "MainThread" "$PILOT_ID" "PMGR_ACTIVE_PENDING" "$msg" \
        | tee -a "$PROFILE"
}


# ------------------------------------------------------------------------------
#
# `waitfor()` will test a condition in certain intervals and return once that
# condition is met, or after a timeout, whichever comes first.  This method will
# not create subshells, and thus can be also used for job control.  It depends
# on `radical-utils-gtod`.
#
waitfor()
{
    INTERVAL="$1"; shift
    TIMEOUT="$1";  shift
    COMMAND="$*"

    START=`echo $(radical-utils-gtod) | cut -f 1 -d .`
    END=$((START + TIMEOUT))
    NOW=$START

    echo "COND start '$COMMAND' (I: $INTERVAL T: $TIMEOUT)"
    while test "$NOW" -lt "$END"
    do
        sleep "$INTERVAL"
        $COMMAND
        RET=$?
        if ! test "$RET" = 0
        then
            echo "COND failed ($RET)"
            break
        else
            echo "COND ok ($RET)"
        fi
        NOW=`echo $(radical-utils-gtod) | cut -f 1 -d .`
    done

    if test "$RET" = 0
    then
        echo "COND timeout"
    fi

    return $RET
}


# ------------------------------------------------------------------------------
# contains(string, substring)
#
# Returns 0 if the specified string contains the specified substring,
# otherwise returns 1.
#
contains()
{
    string="$1"
    substring="$2"
    if test "${string#*$substring}" != "$string"
    then
        return 0    # $substring is in $string
    else
        return 1    # $substring is not in $string
    fi
}


# ------------------------------------------------------------------------------
#
# run a command, log command line and I/O, return success/failure
#
run_cmd()
{
    ret=0
    msg="$1"
    cmd="$2"
    fallback="$3"

    echo ""
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# $msg"
    echo "# cmd: '$cmd'"
    echo "#"

    eval "$cmd" 2>&1

    if test "$?" = 0
    then
        echo "#"
        echo "# SUCCESS"

    else
        echo "#"
        echo "# ERROR"

        if test -z "$3"
        then
            echo "# no fallback command available"
            ret=1

        else
            echo "# running fallback command:"
            echo "# '$fallback'"
            echo "#"
            eval "$fallback" 2>&1

            if test "$?" = 0
            then
                echo "#"
                echo "# SUCCESS (fallback)"

            else
                echo "#"
                echo "# ERROR (fallback)"
                ret=1
            fi
        fi
    fi

    echo "#"
    echo "# -------------------------------------------------------------------"
    return $ret
}


# ------------------------------------------------------------------------------
#
virtenv_activate()
{
    # FIXME: we don't have gtod yet 
    profile_event 've_activate_start'

    virtenv="$1"
    python_dist="$2"

    if test "$python_dist" = "anaconda"
    then
        source activate $virtenv/
        # FIXME: verify
    else
        . "$virtenv/bin/activate"
        if test -z "$VIRTUAL_ENV"
        then
            echo "Loading of virtual env failed!"
            exit 1
        fi

    fi
    TIME_ZERO=$(radical-utils-gtod)
    export TIME_ZERO

    # make sure the lib path into the prefix conforms to the python conventions
    PYTHON_VERSION=`$PYTHON -c 'import distutils.sysconfig as sc; print sc.get_python_version()'`
    echo "PYTHON INTERPRETER: $PYTHON"
    echo "PYTHONPATH        : $PYTHONPATH"
    echo "PYTHON_VERSION    : $PYTHON_VERSION"
    echo "PIP installer     : $PIP"
    echo "PIP version       : `$PIP --version`"
    echo "VIRTENV           : $virtenv"

    profile_event 've_activate_stop'
}


# ------------------------------------------------------------------------------
# If the host that will run the agent is not capable of communication
# with the outside world directly, we will setup a tunnel.
# FIXME: this should be an RU tool
get_tunnel()
{
    addr=$1

    profile_event 'tunnel_setup_start'

    echo "# -------------------------------------------------------------------"
    echo "# Setting up forward tunnel to $addr."

    # Bind to localhost
    BIND_ADDRESS=$(/sbin/ifconfig $TUNNEL_BIND_DEVICE|grep "inet addr"|cut -f2 -d:|cut -f1 -d" ")

    if test -z "$BIND_ADDRESS"
    then
        BIND_ADDRESS=$(/sbin/ifconfig lo | grep 'inet' | xargs echo | cut -f 2 -d ' ')
    fi

    if test -z "$BIND_ADDRESS"
    then
        BIND_ADDRESS=$(ip addr 
                     | grep 'state UP' -A2 
                     | grep 'inet' 
                     | awk '{print $2}' 
                     | cut -f1 -d'/')
    fi

    # find a local port to bind to
    min=23000
    max=23100
    used=" $(netstat -aln    | grep tcp | sed -e 's/\s\s*/ /g' \
           | cut -f 4 -d ' ' | rev      | cut -f 1 -d :        \
           | rev             | sort -un | xargs echo) "
    port=$min
    while test $port -lt $max
    do
        # check if the port in $used
        echo $used | grep " $port " > /dev/null
        if test "$?" = "0"
        then
            port=$((port+1))
            continue
        fi
        DBPORT=$port
        break
    done

    if test -z "$DBPORT"
    then
        echo "# No port found"
        exit 1
    else
        echo "# using port: $DBPORT"
    fi

    # Set up tunnel
    FORWARD_TUNNEL_ENDPOINT_PORT=22

    if test -z "$FORWARD_TUNNEL_ENDPOINT"
    then
        FORWARD_TUNNEL_ENDPOINT_HOST=$BIND_ADDRESS

    elif test "$FORWARD_TUNNEL_ENDPOINT" = "BIND_ADDRESS"; then
        # On some systems, e.g. Hopper, sshd on the mom node is not bound to 127.0.0.1
        # In those situations, and if configured, bind to the just obtained bind address.
        FORWARD_TUNNEL_ENDPOINT_HOST=$BIND_ADDRESS

    else
        FORWARD_TUNNEL_ENDPOINT_HOST=$FORWARD_TUNNEL_ENDPOINT
    fi

    ssh -o StrictHostKeyChecking=no -x -a -4 -T -N \
        -L $BIND_ADDRESS:$DBPORT:$addr \
        -p $FORWARD_TUNNEL_ENDPOINT_PORT $FORWARD_TUNNEL_ENDPOINT_HOST &

    # Kill ssh process when bootstrap_0 dies, to prevent lingering ssh's
    trap 'jobs -p | grep ssh | xargs kill' EXIT

    # and export to agent
    export RP_BS_TUNNEL="$BIND_ADDRESS:$DBPORT"

    profile_event 'tunnel_setup_stop'
}

# -------------------------------------------------------------------------------
#
# run a pre_bootstrap_0 command -- and exit if it happens to fail
#
# pre_bootstrap_0 commands are executed right in arg parser loop
# ( -e can be passed multiple times)
#
pre_bootstrap_0()
{
    cmd="$@"
    run_cmd "Running pre_bootstrap_0 command" "$cmd"

    if test $? -ne 0
    then
        echo "#ABORT"
        exit 1
    fi
}

# -------------------------------------------------------------------------------
#
# Build the PREBOOTSTRAP2 variable to pass down to sub-agents
#
pre_bootstrap_1()
{
    cmd="$@"

    PREBOOTSTRAP2="$PREBOOTSTRAP2
$cmd"
}


# ------------------------------------------------------------------------------
#
# MAIN
#
# Report where we are, as this is not always what you expect ;-)
# Print environment, useful for debugging
echo "---------------------------------------------------------------------"
echo "bootstrap_0 running on host: `hostname -f`."
echo "bootstrap_0 started as     : '$0" "$@"
echo

# parse command line arguments
#
# OPTIONS:
#    -a   session sandbox
#    -b   python distribution (default, anaconda)
#    -c   ccm mode of agent startup
#    -e   execute commands before bootstrapping phase 1: the main agent
#    -f   tunnel forward endpoint (MongoDB host:port)
#    -h   hostport to create tunnel to
#    -i   python Interpreter to use, e.g., python2.7
#    -p   pilot ID
#    -s   session ID
#    -t   tunnel device for connection forwarding
#    -v   virtualenv location
#    -w   execute commands before bootstrapping phase 2: the worker
#    -x   exit cleanup - delete pilot sandbox after completion
# 
while getopts "a:b:ce:f:h:i:p:s:t:v:w:x:" OPTION; do
    case $OPTION in
        a)  SESSION_SANDBOX="$OPTARG"  ;;
        b)  PYTHON_DIST="$OPTARG"  ;;
        c)  CCM='TRUE'  ;;
        e)  pre_bootstrap_0 "$OPTARG"  ;;
        f)  FORWARD_TUNNEL_ENDPOINT="$OPTARG"  ;;
        h)  HOSTPORT="$OPTARG"  ;;
        i)  PYTHON="$OPTARG"  ;;
        p)  PILOT_ID="$OPTARG"  ;;
        s)  SESSION_ID="$OPTARG"  ;;
        t)  TUNNEL_BIND_DEVICE="$OPTARG" ;;
        v)  VIRTENV=$(eval echo "$OPTARG")  ;;
        w)  pre_bootstrap_1 "$OPTARG"  ;;
        x)  CLEANUP="$OPTARG"  ;;
        *)  echo "Unknown option: '$OPTION'='$OPTARG'"
            return 1;;
    esac
done

# pre-execs are done, ready to activate the virtenv
virtenv_activate "$VIRTENV" "$PYTHON_DIST"

# NOTE:  now we have access to radical-utils-gtod
# FIXME: By now the pre_process rules are already performed.
#        We should split the parsing and the execution of those.
#        "bootstrap start" is here so that $PILOT_ID is known.
# Create header for profile log
profile_event 'bootstrap_0_start'  


# derive some var names from given args
if test -z "$SESSION_SANDBOX"
then  
    SESSION_SANDBOX="$PILOT_SANDBOX/.."
fi

LOGFILES_TARBALL="$PILOT_ID.log.tgz"
PROFILES_TARBALL="$PILOT_ID.prof.tgz"

# some backends (condor) never finalize a job when output files are missing --
# so we touch them here to prevent that
echo "# -------------------------------------------------------------------"
echo '# Touching output tarballs'
echo "# -------------------------------------------------------------------"
touch "$LOGFILES_TARBALL"
touch "$PROFILES_TARBALL"


# At this point, all pre_bootstrap_0 commands have been executed.  We copy the
# resulting PATH and LD_LIBRARY_PATH, and apply that in bootstrap_2.sh, so that
# the sub-agents start off with the same env (or at least the relevant parts of
# it).
#
# This assumes that the env is actually transferrable.  If that assumption
# breaks at some point, we'll have to either only transfer the incremental env
# changes, or reconsider the approach to pre_bootstrap_x commands altogether --
# see comment in the pre_bootstrap_0 function.
PB1_PATH="$PATH"
PB1_LDLB="$LD_LIBRARY_PATH"

# Check that mandatory arguments are set
# (Currently all that are passed through to the agent)
if test -z "$PILOT_ID"    ; then  echo "missing PILOT_ID"  ; return 1;  fi


if ! test -z "$FORWARD_TUNNEL_ENDPOINT"
then
    get_tunnel "$HOSTPORT"
    export RADICAL_PILOT_DB_HOSTPORT="$RP_BS_TUNNEL"
fi

# we also set up a tunnel for the application to use, if a respective endpoint
# is requested in the environment
# FIXME: make parameter, connect to bridges, document
if ! test -z "$RP_APP_TUNNEL_ADDR"
then
    echo "app tunnel addr : $RP_APP_TUNNEL_ADDR"
    get_tunnel "$RP_APP_TUNNEL_ADDR"
    export RP_APP_TUNNEL="$RP_BS_TUNNEL"
    echo "app tunnel setup: $RP_APP_TUNNEL"
fi


# ------------------------------------------------------------------------------
# launch the radical agent
#

# At this point we expand the variables in $PREBOOTSTRAP2 to pick up the
# changes made by the environment by pre_bootstrap_0.
OLD_IFS=$IFS
IFS=$'\n'
for entry in $PREBOOTSTRAP2
do
    converted_entry=`eval echo $entry`
    PREBOOTSTRAP2_EXPANDED="$PREBOOTSTRAP2_EXPANDED
$converted_entry"
done
IFS=$OLD_IFS


# we can't always lookup the ntp pool on compute nodes -- so do it once here,
# and communicate the IP to the agent.  The agent may still not be able to
# connect, but then a sensible timeout will kick in on ntplib.
RADICAL_PILOT_NTPHOST=`dig +short 0.pool.ntp.org | grep -v -e ";;" -e "\.$" | head -n 1`
if test -z "$RADICAL_PILOT_NTPHOST"
then
    RADICAL_PILOT_NTPHOST="46.101.140.169"
fi
echo "ntphost: $RADICAL_PILOT_NTPHOST"
ping -c 1 "$RADICAL_PILOT_NTPHOST"

# Before we start the (sub-)agent proper, we'll create a bootstrap_2.sh script
# to do so.  For a single agent this is not needed -- but in the case where
# we spawn out additional agent instances later, that script can be reused to
# get proper # env settings etc, w/o running through bootstrap_0 again.
# That includes pre_exec commands, virtualenv settings and sourcing (again),
# and startup command).
# We don't include any error checking right now, assuming that if the commands
# worked once to get to this point, they should work again for the next agent.
# Famous last words, I know...
# Arguments to that script are passed on to the agent, which is specifically
# done to distinguish agent instances.

# NOTE: anaconda only supports bash.  Really.  I am not kidding...
if test "$PYTHON_DIST" = "anaconda"
then
    BS_SHELL='/bin/bash'
else
    BS_SHELL='/bin/sh'
fi

cat > bootstrap_2.sh <<EOT
#!$BS_SHELL

# disable user site packages as those can conflict with our virtualenv
export PYTHONNOUSERSITE=True

# make sure we use the correct sandbox
cd $PILOT_SANDBOX

# apply some env settings as stored after running pre_bootstrap_0 commands
export PATH="$PB1_PATH"
export LD_LIBRARY_PATH="$PB1_LDLB"

# activate virtenv
if test "$PYTHON_DIST" = "anaconda"
then
    source activate $VIRTENV/
else
    . $VIRTENV/bin/activate
fi

# make sure rp_install is used
export PYTHONPATH=$PYTHONPATH

# run agent in debug mode
# FIXME: make option again?
export RADICAL_LOG_LVL=DEBUG

# avoid ntphost lookups on compute nodes
export RADICAL_PILOT_NTPHOST=$RADICAL_PILOT_NTPHOST

# pass environment variables down so that module load becomes effective at
# the other side too (e.g. sub-agents).
$PREBOOTSTRAP2_EXPANDED

# start agent, forward arguments
# NOTE: exec only makes sense in the last line of the script
exec radical-pilot-agent "\$1" 1>"\$1.out" 2>"\$1.err"

EOT
chmod 0755 bootstrap_2.sh
# ------------------------------------------------------------------------------

#
# Create a barrier to start the agent.
# This can be used by experimental scripts to push all units to the DB before
# the agent starts.
#
if ! test -z "$RADICAL_PILOT_BARRIER"
then
    echo
    echo "# -------------------------------------------------------------------"
    echo "# Entering barrier for $RADICAL_PILOT_BARRIER ..."
    echo "# -------------------------------------------------------------------"

    profile_event 'client_barrier_start'

    while ! test -f $RADICAL_PILOT_BARRIER
    do
        sleep 1
    done

    profile_event 'client_barrier_stop'

    echo
    echo "# -------------------------------------------------------------------"
    echo "# Leaving barrier"
    echo "# -------------------------------------------------------------------"
fi

# start the master agent instance (zero)
profile_event 'sync_rel' 'agent_0 start'


# ------------------------------------------------------------------------------
# FIXME PARTITIONING
#
# Here we jump from bootstrap_0.sh straight to bootstrap_2.sh, to start agent_0.
# Once partitions get introduced, we'll insert a `bootstrap_1.py` right here,
# which will do something like this:
#
#   part_cfg   = ru.read_json(sys.argv[1])
#   partitions = rp.LRMS.partition(part_cfg['lrms'])
#   for partition in partitions:
#       ru.sh_callout('./bootstrap_2.sh agent_0 partition.cfg_file &', 
#                     stdout='./%s.agent_0.bootstrap_2.out' % partition.uid, 
#                     stderr='./%s.agent_0.bootstrap_2.err' % partition.uid)
#   pids = dict()
#   while True:
#       alive = False
#       for partition in partitions:
#           uid = partition.uid
#           pid = pids.get(uid)
#           if not pid:
#               if not os.path.is_file('%s.pid' % uid):
#                   continue
#               else:
#                   with open('%s.pid' % uid, 'r') as fin:
#                       pid = int(fin.read().strip())
#                       pids[uid] = pid
#           if os.kill(pid, 0):
#               # process is alive - that's enough checking for now
#               alive = True
#               break
#       if not alive:
#           sys.exit()
#       time.sleep(1)
# 
# The static `LRMS.partition` method is responsible for splitting up the
# allocation into smaller ones according to the partitioning scheme, in a way
# that the same LRMS can then pick thos up during `agent_0` startup, resulting
# in a functional agent on that partition.  Note that all agents live on the
# same (*this*) node, so this will only be doable for a finite (aka 'small') set
# of partitions.
#
# FIXME: do we need to use bootstrap_2.sh to start bootstrap_1.py? 
#       
#
if test -z "$CCM"; then
    ./bootstrap_2.sh 'agent_0'    \
                   1> agent_0.bootstrap_2.out \
                   2> agent_0.bootstrap_2.err &
else
    ccmrun ./bootstrap_2.sh 'agent_0'    \
                   1> agent_0.bootstrap_2.out \
                   2> agent_0.bootstrap_2.err &
fi
AGENT_PID=$!

while true
do
    test -z "$AGENT_PID" && break

    sleep 1
    if kill -0 $AGENT_PID 2>/dev/null
    then 
        if test -e "./killme.signal"
        then
            profile_event 'killme'
            profile_event 'sigterm'
            echo "send SIGTERM to $AGENT_PID ($$)"
            kill -15 $AGENT_PID
            waitfor 1 30 "kill -0  $AGENT_PID"
            test "$?" = 0 || break

            profile_event 'sigkill'
            echo "send SIGKILL to $AGENT_PID ($$)"
            kill  -9 $AGENT_PID
        fi
    else 
        profile_event 'agent_gone'
        echo "agent $AGENT_PID is gone"
        break
    fi
done

# collect process and exit code
echo "agent $AGENT_PID is final"
wait $AGENT_PID
AGENT_EXITCODE=$?
echo "agent $AGENT_PID is final ($AGENT_EXITCODE)"
profile_event 'agent_final' "$AGENT_PID:$AGENT_EXITCODE"


# cleanup flags:
#   l : pilot log files
#   u : unit work dirs
#   v : virtualenv
#   e : everything
echo
echo "# -------------------------------------------------------------------"
echo "# CLEANUP: $CLEANUP"
echo "#"

profile_event 'cleanup_start'
contains $CLEANUP 'e' && rm -r "$PILOT_SANDBOX/"
profile_event 'cleanup_stop'

echo "#"
echo "# -------------------------------------------------------------------"

if test -z "`ls *.prof 2>/dev/null`"
then
    touch $PROFILES_TARBALL
else
    echo
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# Mark final profiling entry ..."
    profile_event 'bootstrap_0_stop'
    profile_event 'END'
    echo "#"
    echo "# -------------------------------------------------------------------"
    echo
    FINAL_SLEEP=5
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# We wait for some seconds for the FS to flush profiles."
    echo "# Success is assumed when all profiles end with a 'END' event."
    echo "#"
    echo "# -------------------------------------------------------------------"
    nprofs=`echo *.prof | wc -w`
    nend=`tail -n 1 *.prof | grep END | wc -l`
    nsleep=0
    while ! test "$nprofs" = "$nend"
    do
        nsleep=$((nsleep+1))
        if test "$nsleep" = "$FINAL_SLEEP"
        then
            echo "abort profile sync @ $nsleep: $nprofs != $nend"
            break
        fi
        echo "delay profile sync @ $nsleep: $nprofs != $nend"
        sleep 1
        # recheck nprofs too, just in case...
        nprofs=`echo *.prof | wc -w`
        nend=`tail -n 1 *.prof | grep END | wc -l`
    done
    echo "nprofs $nprofs =? nend $nend"
    date
    echo
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# Tarring profiles ..."
    tar -czf $PROFILES_TARBALL.tmp *.prof || true
    mv $PROFILES_TARBALL.tmp $PROFILES_TARBALL
    ls -l $PROFILES_TARBALL
    echo "#"
    echo "# -------------------------------------------------------------------"
fi

if test -z "`ls *{log,out,err,cfg} 2>/dev/null`"
then
    touch $LOGILES_TARBALL
else
    # TODO: This might not include all logs, as some systems only write
    #       the output from the bootstrapper once the jobs completes.
    echo
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# Tarring logfiles ..."
    tar -czf $LOGFILES_TARBALL.tmp *.{log,out,err,cfg} || true
    mv $LOGFILES_TARBALL.tmp $LOGFILES_TARBALL
    ls -l $LOGFILES_TARBALL
    echo "#"
    echo "# -------------------------------------------------------------------"
fi

echo "# -------------------------------------------------------------------"
echo "#"
if test -e "./killme.signal"
then
    # this agent died cleanly, and we can rely on thestate information given.
    final_state=$(cat ./killme.signal)
    if ! test "$AGENT_EXITCODE" = "0"
    then
        echo "changing exit code from $AGENT_EXITCODE to 0 for canceled pilot"
        AGENT_EXITCODE=0
    fi
fi

if test -z "$final_state"
then
    # assume this agent died badly
    echo 'reset final state to FAILED'
    final_state='FAILED'
fi

echo "# -------------------------------------------------------------------"
echo "# push final pilot state: $SESSION_ID $PILOT_ID $final_state"
sp=$(which radical-pilot-agent-statepush)
test -z "$sp" && echo "statepush not found"
test -z "$sp" || $PYTHON "$sp" agent_0.cfg "$final_state"

echo
echo "# -------------------------------------------------------------------"
echo "#"
echo "# Done, exiting ($AGENT_EXITCODE)"
echo "#"
echo "# -------------------------------------------------------------------"

# ... and exit
exit $AGENT_EXITCODE

