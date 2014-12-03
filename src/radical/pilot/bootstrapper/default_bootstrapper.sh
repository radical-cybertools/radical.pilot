#!/bin/bash -l

# ------------------------------------------------------------------------------
# Copyright 2013-2014, radical@rutgers.edu
# License under the MIT License
#
# This script launches a radical.pilot compute pilot.
#

# ------------------------------------------------------------------------------
# global variables
#
AUTH=
CLEANUP=
CORES=
DBNAME=
DBURL=
DEBUG=
VIRTENV=
VIRTENV_MODE=
LRMS=
MPI_LAUNCH_METHOD=
PILOT_ID=
PILOT_VERSION=
PILOT_TYPE=
PYTHON=
RUNTIME=
SCHEDULER=
SESSIONID=
TASK_LAUNCH_METHOD=
SANDBOX=`pwd`


# seconds to wait for lock files 
# 5 min should be enough for anybody to create/update a virtenv...
LOCK_TIMEOUT=30
VIRTENV_TGZ_URL="https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz"
VIRTENV_TGZ="virtualenv-1.9.tar.gz"
VIRTENV_IS_ACTIVATED=FALSE


# --------------------------------------------------------------------
#
# it is suprisingly difficult to get seconds since epoch in POSIX -- 
# 'date +%s' is a GNU extension...  Anyway, awk to the rescue! 
#
timestamp () {
  TIMESTAMP=`\awk 'BEGIN{srand(); print srand()}'`
}


if ! test -z "$RADICAL_PILOT_PROFILE"
then
    timestamp
    TIME_ZERO=$TIMESTAMP
    export TIME_ZERO
fi


# ------------------------------------------------------------------------------
#
profile_event()
{
    if ! test -z "$RADICAL_PILOT_PROFILE"
    then
        timestamp
        NOW=$((TIMESTAMP-TIME_ZERO))
        printf '  %12s : %-20s : %12.4f : %-15s : %-24s : %-40s : \n' \
                  ' '    ' '     "$NOW"   ' '     'Bootstrap' "$@"    \
        >> AGENT.prof
    fi
}

profile_event 'bootstrap start'


# ------------------------------------------------------------------------------
#
# some virtenv operations need to be protected against pilots starting up 
# concurrently.
#
# I/O redirect under noclobber is atomic in POSIX
#
lock()
{
    pid="$1"      # ID of pilot/bootstrapper waiting
    entry="$2"    # entry to lock
    timeout="$3"  # time to wait for a lock to expire In seconds)

    # clean $entry (normalize path, remove trailing slash, etc
    entry="`dirname $entry`/`basename $entry`"

    if test -z $timeout
    then
        timeout=$LOCK_TIMEOUT
    fi

    lockfile="$entry.lock"
    count=0

    set -C
    until echo $pid 2>/dev/null >$lockfile
    do       
        owner=`cat $lockfile 2>/dev/null`
        count=$((count+1))

        echo "wait for lock $lockfile (owned by $owner) $((timeout-count))"

        if test $count -gt $timeout
        then
            echo "lock timeout for $entry -- removing stale lock for '$owner'"
            rm $lockfile
            # we do not exit the loop here, but race again against other pilots
            # waiting for this lock.
            count=0
        else

            # need to wait longer for lock release
            sleep 1
        fi
    done

    # one way or the other, we got the lock finally.  Reset noclobber option and
    # return
    set +C
}


# ------------------------------------------------------------------------------
#
# remove an previously qcquired lock.  This will abort if the lock is already
# gone, or if it is not owned by us -- both cases indicate that a different
# pilot got tired of waiting for us and forcefully took over the lock
#
unlock()
{
    pid="$1"      # ID of pilot/bootstrapper which has the lock 
    entry="$2"    # locked entry

    # clean $entry (normalize path, remove trailing slash, etc
    entry="`dirname $entry`/`basename $entry`"

    lockfile="$entry.lock"


    if ! test -f $lockfile
    then
        echo "ERROR: cannot unlock $entry for $pid: missing lock $lockfile"
        exit 1
    fi

    owner=`cat $lockfile`
    if ! test "$owner" = "`echo $pid`"
    then
        echo "ERROR: cannot unlock $entry for $pid: owner is $owner"
        exit 1
    fi

    rm $lockfile
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
    msg="$1"
    cmd="$2"
    fallback="$3"

    echo ""
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# $msg"
    echo "# cmd: $cmd"
    echo "#"
    $cmd
    if test "$?" = 0
    then
        echo "#"
        echo "# SUCCESS"
        echo "#"
        echo "# -------------------------------------------------------------------"
        return 0
    else
        echo "#"
        echo "# ERROR"

        if test -z "$3"
        then
            echo "# no fallback command available"
        else
            echo "# running fallback command:"
            echo "# $fallback"
            echo "#"
            $fallback
            if test "$?" = 0
            then
                echo "#"
                echo "# SUCCESS (fallback)"
                echo "#"
                echo "# -------------------------------------------------------------------"
                return 0
            else
                echo "#"
                echo "# ERROR (fallback)"
            fi
        fi
        echo "#"
        echo "# -------------------------------------------------------------------"
        return 1
    fi
}


# ------------------------------------------------------------------------------
# print out script usage help
#
usage()
{
    msg="$@"

    if ! test -z "$msg"
    then
        printf "\n\tERROR: $msg\n\n"
    fi

    cat << EOF >> /dev/stderr
usage: $0 options

This script launches a RADICAL-Pilot agent.

OPTIONS:
   -a      The name of project / allocation to charge.
   -c      Number of requested cores.
   -d      Specify debug level.
   -e      List of commands to run before bootstrapping.
   -f      Tunnel endpoint for connection forwarding.
   -g      Global shared virtualenv (create if missing)
   -h      Show this message.
   -i      The Python interpreter to use, e.g., python2.7.
           (default is '/usr/bin/python')
   -j      Task launch method.
   -k      MPI launch method.
   -l      Type of Local Resource Management System.
   -m      Address and port of the coordination service host (MongoDB).
   -n      The name of the database.
   -p      The unique identifier (uid) of the pilot.
   -q      The scheduler to be used by the agent.
   -s      The unique identifier (uid) of the session.
   -r      Runtime in minutes.
   -u      sandbox is user defined
   -v      Version - the RADICAL-Pilot version to install in virtenv
   -w      The working directory (sandbox) of the pilot.
           (default is '.')
   -x      Cleanup - delete pilot sandbox, virtualenv etc. after completion

EOF

    # On error message, exit with error code
    if ! test -z "$msg"
    then
        exit 1
    fi
}


# ------------------------------------------------------------------------------
#
# create and/or update a virtenv, depending on mode specifier:
#
#   'private' : error  if it exists, otherwise create, then use
#   'update'  : update if it exists, otherwise create, then use
#   'create'  : use    if it exists, otherwise create, then use
#   'use'     : use    if it exists, otherwise error,  then exit
#
# create and update ops will be locked and thus protected against concurrent
# bootstrapper invokations.  
#
# That locking will likely not scale nicely for larger numbers of concurrent
# pilot, at least not for slow running updates (time for update of n pilots
# needs to be smaller than lock timeout).  OTOH, concurrent pip updates should
# not have a negative impact on the virtenv in the first place, AFAIU -- lock on
# create is more important, and should be less critical
# 
setup_virtenv()
{
    pid="$1"
    virtenv="$2"
    virtenv_mode="$3"

    virtenv_create=TRUE
    virtenv_update=TRUE

    lock "$pid" "$virtenv" # use default timeout

    if test "$virtenv_mode" = "private"
    then
        if test -d "$virtenv"
        then
            printf "\nERROR: private virtenv already exists at $virtenv\n\n"
            unlock "$pid" "$virtenv"
            exit 1
        fi
        virtenv_create=TRUE
        virtenv_update=FALSE
    
    elif test "$virtenv_mode" = "update"
    then
        test -d "$virtenv" || virtenv_create=TRUE
        virtenv_update=TRUE

    elif test "$virtenv_mode" = "create"
    then
        virtenv_create=TRUE
        virtenv_update=FALSE

    elif test "$virtenv_mode" = "use"
    then
        if ! test -s "$virtenv"
        then
            printf "\nERROR: given virtenv does not exists at $virtenv\n\n"
            unlock "$pid" "$virtenv"
            exit 1
        fi
        virtenv_create=FALSE
        virtenv_update=FALSE

    fi

    echo "virtenv_create   : $virtenv_create"
    echo "virtenv_update   : $virtenv_update"


    # radical_pilot installation and update is governed by PILOT_VERSION.  If
    # that is set to 'stage', we install the release and use the pilot which was
    # staged to pwd.  If set to 'release', we install from pypi.  In all other
    # cases, we install from git at a specific tag or branch
    #
    # Note though that some virtenv modes won't be able to cope with specific
    # tag or branch requests (RP_MODE_CHECK)
    #
    if test "$PILOT_VERSION" = 'stage' \
         -o "$PILOT_VERSION" = 'release'
    then
        RP_INSTALL_SOURCE='radical.pilot'
        RP_INSTALL_EASY=TRUE
        RP_MODE_CHECK=FALSE
    else
        RP_INSTALL_SOURCE="-e git://github.com/radical-cybertools/radical.pilot.git@$PILOT_VERSION#egg=radical.pilot"
        RP_INSTALL_EASY=FALSE # easy_install cannot handle git...
        RP_MODE_CHECK=TRUE
    fi

    echo "rp install source: $RP_INSTALL_SOURCE"
    echo "rp install easy  : $RP_INSTALL_EASY"


    # create virtenv if needed.  This also activates the virtenv.
    if test "$virtenv_create" = "TRUE"
    then
        if ! test -d "$virtenv"
        then
            virtenv_create "$virtenv"
            if ! test "$?" = 0
            then
               echo "Error on virtenv creation -- abort"
               unlock "$pid" "$virtenv"
               exit 1
            fi
        else
            echo "virtenv $virtenv exists"
        fi
    else
        echo "do not create virtenv $virtenv"
        if test "$RP_MODE_CHECK" = "TRUE"
        then
            echo "WARNING: the requested pilot version '$PILOT_VERSION' make not be available!"
        fi
    fi

    # creation or not -- at this point it needs activation
    if test "$VIRTENV_IS_ACTIVATED" = "FALSE"
    then
        source "$virtenv/bin/activate"
        VIRTENV_IS_ACTIVATED=TRUE
    fi

    
    # update virtenv if needed.  This also activates the virtenv.
    if test "$virtenv_update" = "TRUE"
    then
        virtenv_update "$virtenv"
        if ! test "$?" = 0
        then
           echo "Error on virtenv update -- abort"
           unlock "$pid" "$virtenv"
           exit 1
       fi
    else
        echo "do not update virtenv $virtenv"
        if test "$RP_MODE_CHECK" = "TRUE"
        then
            echo "WARNING: the requested pilot version '$PILOT_VERSION' make not be available!"
        fi
    fi

    unlock "$pid" "$virtenv"
}


# ------------------------------------------------------------------------------
#
# create virtualenv - we always use the latest version from GitHub
#
# The virtenv creation will alson install the required packges, but will *not*
# use '--upgrade', so that will become a noop if the packages have been
# installed before.  An eventual upgrade will be triggered independently in
# virtenv_update().
#
virtenv_create()
{
    profile_event 'virtenv_create start'

    VIRTENV="$1"

    # create a fresh virtualenv. we use an older 1.9.x version of 
    # virtualenv as this seems to work more reliable than newer versions.
    # If we can't download, we try to move on with the system virtualenv.
    run_cmd "Download virtualenv tgz" \
            "curl -k -O '$VIRTENV_TGZ_URL'"

    if ! test "$?" = 0 
    then
        echo "WARNING: Couldn't download virtualenv via curl! Using system version."
        BOOTSTRAP_CMD="virtualenv $VIRTENV"

    else :
        run_cmd "unpacking virtualenv tgz" \
                "tar xvfz '$VRTENV_TGZ'"

        if test $? -ne 0 
        then
            echo "Couldn't unpack virtualenv!"
            return 1
        fi
        
        BOOTSTRAP_CMD="$PYTHON virtualenv-1.9/virtualenv.py $VIRTENV"
    fi


    run_cmd "Create virtualenv" \
            "$BOOTSTRAP_CMD"
    if test $? -ne 0 
    then
        echo "Couldn't create virtualenv"
        return 1
    fi

    # activate the virtualenv
    source $VIRTENV/bin/activate
    VIRTENV_IS_ACTIVATED=TRUE
    

    run_cmd "Downgrade pip to 1.2.1" \
            "easy_install pip==1.2.1"
    if test $? -ne 0
    then
        echo "Couldn't downgrade pip! Using default version (if it exists)"
    fi

    
    # run_cmd "update setuptools" \
    #         "pip install --upgrade setuptools"
    # if test $? -ne 0 
    # then
    #     echo "Couldn't update setuptools -- using default version"
    # fi

    
    # On india/fg 'pip install saga-python' does not work as pip fails to
    # install apache-libcloud (missing bz2 compression).  We thus install that
    # dependency via easy_install.
    run_cmd "install apache-libcloud" \
            "easy_install --upgrade apache-libcloud"
    if test $? -ne 0 
    then
        echo "Couldn't install/upgrade apache-libcloud! Lets see how far we get ..."
    fi

    
    echo "Using RADICAL-Pilot installation source '$RP_INSTALL_SOURCE'"

    if test "$RP_INSTALL_EASY" = 'TRUE'
    then
        run_cmd "install radical.pilot via pip/easy_install" \
                "pip install  $RP_INSTALL_SOURCE" \
                "easy_install $RP_INSTALL_SOURCE"
    else
        run_cmd "install radical.pilot via pip" \
                "pip install  $RP_INSTALL_SOURCE"
    fi
    if test $? -ne 0 
    then
        echo "Couldn't install radical.pilot! Lets see how far we get ..."
    fi

    profile_event 'virtenv_create done'
}


# ------------------------------------------------------------------------------
#
# update virtualenv - this assumes that the virtenv has been activated
#
virtenv_update()
{
    profile_event 'virtenv_update start'

    # we first uninstall radical pilot, so that any request for a specific
    # version can be honored even if the version is lower than what is
    # installed.  Failure to do so will only result in a warning though.
    echo "uninstalling RADICAL-Pilot"
    run_cmd "uninstall radical.pilot via pip" \
            "yes | head -n 1 | pip uninstall radical.pilot"
    if test $? -ne 0 
    then
        echo "Couldn't uninstall radical.pilot! Lets see how far we get ..."
    fi

    echo "Using RADICAL-Pilot update source '$RP_INSTALL_SOURCE'"

    if test "$RP_INSTALL_EASY" = 'TRUE'
    then
        run_cmd "update radical.pilot via pip/easy_install" \
                "pip install  --upgrade $RP_INSTALL_SOURCE" \
                "easy_install --upgrade $RP_INSTALL_SOURCE"
    else
        run_cmd "update radical.pilot via pip" \
                "pip install  --upgrade $RP_INSTALL_SOURCE"
    fi
    if test $? -ne 0 
    then
        echo "Couldn't upgrade radical.pilot! Lets see how far we get ..."
    fi

    profile_event 'virtenv_update done'
}


# ------------------------------------------------------------------------------
# Find available port on the remote host where we can bind to
#
find_available_port()
{
    RANGE="23000..23100"
    echo ""
    echo "################################################################################"
    echo "## Searching for available TCP port for tunnel in range $RANGE."
    host=$1
    for port in $(eval echo {$RANGE}); do

        # Try to make connection
        (bash -c "(>/dev/tcp/$host/$port)" 2>/dev/null) &
        # Wait for 1 second
        read -t1
        # Kill child
        kill $! 2>/dev/null
        # If the kill command succeeds, assume that we have found our match!
        if [ "$?" == "0" ]
        then
            break
        fi

        # Reset port, so that the last port doesn't get chosen in error
        port=
    done

    # Wait for children
    wait 2>/dev/null

    # Assume the most recent port is available
    AVAILABLE_PORT=$port
}


# -------------------------------------------------------------------------------
#
# run a preprocess command -- and exit if it happens to fail
#
# preprocess commands are executed right in arg parser loop because -e can be 
# passed multiple times
#
preprocess()
{
    cmd="$@"
    run_cmd  "Running pre-process command" "$@"

    if test $? -ne 0 
    then
        echo "#ABORT"
        exit 1
    fi
}

    
# ------------------------------------------------------------------------------
#
# MAIN 
#

# Report where we are, as this is not always what you expect ;-)
# Print environment, useful for debugging
echo "# -------------------------------------------------------------------"
echo "# Bootstrapper running on host: `hostname -f`."
echo "# Bootstrapper started as     : '$0 $@'"
echo "# Environment of bootstrapper process:"
echo "#"
echo "#"
printenv
echo "# -------------------------------------------------------------------"

# parse command line arguments
# free letters: b h o
while getopts "a:c:d:e:f:g:hi:j:k:l:m:n:p:q:r:u:s:t:v:w:x:y:z:" OPTION; do
    PRE_PROCESS=
    case $OPTION in
        a)  AUTH=$OPTARG  ;;
        c)  CORES=$OPTARG  ;;
        d)  DEBUG=$OPTARG  ;;
        e)  preprocess $OPTARG  ;;
        f)  FORWARD_TUNNEL_ENDPOINT=$OPTARG  ;;
        g)  VIRTENV=$OPTARG  ;;
        i)  PYTHON=$OPTARG  ;;
        j)  TASK_LAUNCH_METHOD=$OPTARG  ;;
        k)  MPI_LAUNCH_METHOD=$OPTARG  ;;
        l)  LRMS=$OPTARG  ;;
        m)  DBURL=$OPTARG   ;;
        n)  DBNAME=$OPTARG  ;;
        p)  PILOT_ID=$OPTARG  ;;
        q)  SCHEDULER=$OPTARG  ;;
        r)  RUNTIME=$OPTARG  ;;
        s)  SESSIONID=$OPTARG  ;;
        t)  PILOT_TYPE=$OPTARG  ;;
        u)  VIRTENV_MODE=$OPTARG  ;;
        v)  PILOT_VERSION=$OPTARG  ;;
        w)  SANDBOX=$OPTARG  ;;
        x)  CLEANUP=$OPTARG  ;;
        *)  usage "Unknown option: $OPTION=$OPTARG"  ;;
    esac
done

# Check that mandatory arguments are set
# (Currently all that are passed through to the agent)
if test -z "$AUTH"               ; then  usage "missing AUTH              ";  fi
if test -z "$CORES"              ; then  usage "missing CORES             ";  fi
if test -z "$DEBUG"              ; then  usage "missing DEBUG             ";  fi
if test -z "$DBNAME"             ; then  usage "missing DBNAME            ";  fi
if test -z "$DBURL"              ; then  usage "missing DBURL             ";  fi
if test -z "$LRMS"               ; then  usage "missing LRMS              ";  fi
if test -z "$MPI_LAUNCH_METHOD"  ; then  usage "missing MPI_LAUNCH_METHOD ";  fi
if test -z "$PILOT_ID"           ; then  usage "missing PILOT_ID          ";  fi
if test -z "$RUNTIME"            ; then  usage "missing RUNTIME           ";  fi
if test -z "$SCHEDULER"          ; then  usage "missing SCHEDULER         ";  fi
if test -z "$SESSIONID"          ; then  usage "missing SESSIONID         ";  fi
if test -z "$TASK_LAUNCH_METHOD" ; then  usage "missing TASK_LAUNCH_METHOD";  fi
if test -z "$PILOT_VERSION"      ; then  usage "missing PILOT_VERSION     ";  fi

# If the host that will run the agent is not capable of communication
# with the outside world directly, we will setup a tunnel.
if [[ $FORWARD_TUNNEL_ENDPOINT ]]; then

    profile_event 'tunnel setup start'

    echo "# -------------------------------------------------------------------"
    echo "# Setting up forward tunnel for MongoDB to $FORWARD_TUNNEL_ENDPOINT."

    find_available_port $FORWARD_TUNNEL_ENDPOINT
    if [ $AVAILABLE_PORT ]; then
        echo "## Found available port: $AVAILABLE_PORT"
    else
        echo "## No available port found!"
        exit 1
    fi
    DBPORT=$AVAILABLE_PORT
    BIND_ADDRESS=127.0.0.1

    # Set up tunnel
    ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:$DBURL $FORWARD_TUNNEL_ENDPOINT &

    # Kill ssh process when bootstrapper dies, to prevent lingering ssh's
    trap 'jobs -p | xargs kill' EXIT

    # Overwrite DBURL
    DBURL=$BIND_ADDRESS:$DBPORT

    profile_event 'tunnel setup done'

fi

# If PYTHON was not set as an argument, detect it here.
if [[ -z "$PYTHON" ]]
then
    PYTHON=`which python`
fi

setup_virtenv "$PILOT_ID" "$VIRTENV" "$VIRTENV_MODE"

# Export the variables related to virtualenv,
# so that we can disable the virtualenv for the cu.
export _OLD_VIRTUAL_PATH
export _OLD_VIRTUAL_PYTHONHOME
export _OLD_VIRTUAL_PS1

# ------------------------------------------------------------------------------
# launch the radical agent
#
# the actual agent script lives in PWD if it was staged -- otherwise we use it
# from the virtenv
if test "$PILOT_VERSION" = 'stage'
then
    PILOT_SCRIPT='./radical-pilot-agent'
else
    PYTHON_PATH=`which python`
    PILOT_SCRIPT="`dirname $PYTHON_PATH`/radical-pilot-agent-$PILOT_TYPE"
fi

AGENT_CMD="python $PILOT_SCRIPT \
    -a $AUTH \
    -c $CORES \
    -d $DEBUG \
    -j $TASK_LAUNCH_METHOD \
    -k $MPI_LAUNCH_METHOD \
    -l $LRMS \
    -m $DBURL \
    -n $DBNAME \
    -p $PILOT_ID \
    -q $SCHEDULER \
    -s $SESSIONID \
    -r $RUNTIME "

echo
echo "# -------------------------------------------------------------------"
echo "# Launching radical-pilot-agent for $CORES cores."
echo "# CMDLINE: $AGENT_CMD"

profile_event 'agent start'

$AGENT_CMD
AGENT_EXITCODE=$?

profile_event 'cleanup start'

# cleanup flags:
#   l : pilot log files
#   u : unit work dirs
#   v : virtualenv
#   e : everything
echo
echo "# -------------------------------------------------------------------"
echo "# CLEANUP: $CLEANUP"
echo "#"
contains $CLEANUP 'l' && echo "rm -r $SANDBOX/AGENT.*"
contains $CLEANUP 'u' && echo "rm -r $SANDBOX/unit-*"
contains $CLEANUP 'v' && echo "rm -r $VIRTENV/"
contains $CLEANUP 'e' && echo "rm -r $SANDBOX/"
# contains $CLEANUP 'l' && rm -r $SANDBOX/AGENT.*
# contains $CLEANUP 'u' && rm -r $SANDBOX/unit-*
# contains $CLEANUP 'v' && rm -r $VIRTENV/
# contains $CLEANUP 'e' && rm -r $SANDBOX/

profile_event 'cleanup done'
echo "#"
echo "# -------------------------------------------------------------------"

# ... and exit
exit $AGENT_EXITCODE

