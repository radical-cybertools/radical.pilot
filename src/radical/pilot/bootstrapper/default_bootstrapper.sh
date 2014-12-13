#!/bin/bash -l

# -----------------------------------------------------------------------------
# Copyright 2013-2014, radical@rutgers.edu
# License under the MIT License
#
# This script launches a radical.pilot compute pilot.
#

# -----------------------------------------------------------------------------
# global variables
#
AUTH=
CLEANUP=
CORES=
DBNAME=
DBURL=
DEBUG=
VIRTENV=
GLOBAL_VIRTENV=
LRMS=
MPI_LAUNCH_METHOD=
PREBOOTSTRAP=
PILOTID=
PYTHON=
RUNTIME=
SESSIONID=
TASK_LAUNCH_METHOD=
VERSION=
SANDBOX=`pwd`

# -----------------------------------------------------------------------------
# contains(string, substring)
#
# Returns 0 if the specified string contains the specified substring,
# otherwise returns 1.
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

# -----------------------------------------------------------------------------
# print out script usage help
#
usage()
{
cat << EOF >> /dev/stderr
usage: $0 options

This script launches a RADICAL-Pilot agent.

OPTIONS:
   -a      The name of project / allocation to charge.
   -b      enable agent benchmarking
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
   -s      The unique identifier (uid) of the session.
   -t      Runtime in minutes.
   -u      sandbox is user defined
   -v      Version - the RADICAL-Pilot package version.
   -w      The working directory (sandbox) of the pilot.
           (default is '.')
   -x      Cleanup - delete pilot sandbox, virtualenv etc. after completion

EOF
}

# -----------------------------------------------------------------------------
# bootstrap virtualenv - we always use the latest version from GitHub
#
installvenv()
{
    # first argument is the virtenv target
    VIRTENV=$1

    # create a fresh virtualenv. we use an older 1.9.x version of 
    # virtualenv as this seems to work more reliable than newer versions.
    # If we can't download, we try to move on with the system virtualenv.
    CURL_CMD="curl -k -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz"
    echo ""
    echo "################################################################################"
    echo "## Downloading and installing virtualenv"
    echo "## CMDLINE: $CURL_CMD"
    $CURL_CMD
    if test $? -ne 0 ; then
        echo "WARNING: Couldn't download virtualenv via curl! Using system version."
        BOOTSTRAP_CMD="virtualenv $VIRTENV"
    else :
        tar xvfz virtualenv-1.9.tar.gz
        if test $? -ne 0 ; then
            echo "Couldn't unpack virtualenv! ABORTING"
            exit 1
        fi
        
        BOOTSTRAP_CMD="$PYTHON virtualenv-1.9/virtualenv.py $VIRTENV"
    fi

    echo ""
    echo "################################################################################"
    echo "## Creating virtualenv"
    echo "## CMDLINE: $BOOTSTRAP_CMD"
    $BOOTSTRAP_CMD
    if test $? -ne 0 ; then
        echo "Couldn't bootstrap virtualenv! ABORTING"
        exit 1
    fi

    # activate the virtualenv
    source $VIRTENV/bin/activate
    
    DOWNGRADE_PIP_CMD="easy_install pip==1.2.1"
    echo ""
    echo "################################################################################"
    echo "## Downgrading pip to 1.2.1"
    echo "## CMDLINE: $DOWNGRADE_PIP_CMD"
    $DOWNGRADE_PIP_CMD
    if test $? -ne 0 ; then
        echo "Couldn't downgrade pip! Using default version (if it exists)"
    fi
    
    #UPDATE_SETUPTOOLS_CMD="pip install --upgrade setuptools"
    #echo ""
    #echo "################################################################################"
    #echo "## Updating virtualenv"
    #echo "## CMDLINE: $UPDATE_SETUPTOOLS_CMD"
    #$UPDATE_SETUPTOOLS_CMD
    #if test $? -ne 0 ; then
    #    echo "Couldn't update virtualenv! ABORTING"
    #    exit 1
    #fi
    
    # On india/fg 'pip install saga-python' does not work as pip fails to
    # install apache-libcloud (missing bz2 compression).  We thus install that
    # dependency via easy_install.
    EI_CMD="easy_install --upgrade apache-libcloud"
    echo ""
    echo "################################################################################"
    echo "## install/upgrade Apache-LibCloud"
    echo "## CMDLINE: $EI_CMD"
    $EI_CMD
    if test $? -ne 0 ; then
        echo "Couldn't install/upgrade apache-libcloud! Lets see how far we get ..."
    fi
    
    # Now pip install should work...
    PIP_CMD="pip install --upgrade saga-python"
    EA_CMD="easy_install --upgrade saga-python"
    echo ""
    echo "################################################################################"
    echo "## install/upgrade SAGA-Python"
    echo "## CMDLINE: $PIP_CMD"
    $PIP_CMD
    if test $? -ne 0 ; then
        echo "pip install failed, trying easy_install ..."
        $EI_CMD
        if test $? -ne 0 ; then
            echo "Couldn't install/upgrade SAGA-Python! Lets see how far we get ..."
        fi
    fi
    
    PIP_CMD="pip install --upgrade python-hostlist"
    EI_CMD="easy_install --upgrade python-hostlist"
    echo ""
    echo "################################################################################"
    echo "## install/upgrade python-hostlist"
    echo "## CMDLINE: $PIP_CMD"
    $PIP_CMD
    if test $? -ne 0 ; then
        echo "pip install failed, trying easy_install ..."
        $EI_CMD
        if test $? -ne 0 ; then
            echo "Easy install failed too, couldn't install python-hostlist!  Lets see how far we get..."
        fi
    fi
    
    # pymongo should be pulled by saga, via utils.  But whatever...
    PIP_CMD="pip install --upgrade pymongo"
    EI_CMD="easy_install --upgrade pymongo"
    echo ""
    echo "################################################################################"
    echo "## install/upgrade pymongo"
    echo "## CMDLINE: $PIP_CMD"
    $PIP_CMD
    if test $? -ne 0 ; then
        echo "pip install failed, trying easy_install ..."
        $EI_CMD
        if test $? -ne 0 ; then
            echo "Easy install failed too, couldn't install pymongo! Oh well..."
        fi
    fi
}

# -----------------------------------------------------------------------------
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
        if [ "$?" == "0" ]; then
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

# -----------------------------------------------------------------------------
# MAIN 
#

# Report where we are, as this is not always what you expect ;-)
echo "################################################################################"
echo "## Bootstrapper running on host: `hostname -f`."
echo "## Bootstrapper started as     : '$0 $*'"

# Print environment, useful for debugging
echo ""
echo "################################################################################"
echo "## Environment of bootstrapper process:"
printenv

# parse command line arguments
USER_SANDBOX=0
BENCHMARK=0
while getopts "a:bc:d:e:f:g:hi:j:k:l:m:n:op:qrs:t:uv:w:x:yz" OPTION; do
    case $OPTION in
        a)
            # Passed to agent
            AUTH=$OPTARG
            ;;
        b)
            # Passed to agent
            BENCHMARK=1
            ;;
        c)
            # Passed to agent
            CORES=$OPTARG
            ;;
        d)
            # Passed to agent
            DEBUG=$OPTARG
            ;;
        e)
            PREBOOTSTRAP=$OPTARG

            # Note: Executed inline here because -e can be passed multiple times.
            echo ""
            echo "################################################################################"
            echo "## Running pre-bootstrapping command"
            echo "## CMDLINE: $PREBOOTSTRAP"
            $PREBOOTSTRAP
            if test $? -ne 0 ; then
                echo "Error running pre-boostrapping command! ABORTING"
                exit 1
            fi
            ;;
        f)
            FORWARD_TUNNEL_ENDPOINT=$OPTARG
            ;;
        g)
            GLOBAL_VIRTENV=$OPTARG
            ;;
        h)
            usage
            exit 1
            ;;
        i)
            PYTHON=$OPTARG
            ;;
        j)
            # Passed to agent
            TASK_LAUNCH_METHOD=$OPTARG
            ;;
        k)
            # Passed to agent
            MPI_LAUNCH_METHOD=$OPTARG
            ;;
        l)
            # Passed to agent
            LRMS=$OPTARG
            ;;
        m)
            # Passed to agent, possibly after rewrite for proxy
            DBURL=$OPTARG
            ;;
        n)
            # Passed to agent
            DBNAME=$OPTARG
            ;;
        p)
            # Passed to agent
            PILOTID=$OPTARG
            ;;
        s)
            # Passed to agent
            SESSIONID=$OPTARG
            ;;
        t)
            # Passed to agent
            RUNTIME=$OPTARG
            ;;
        u)
            USER_SANDBOX=1
            ;;
        v)
            # Passed to agent
            VERSION=$OPTARG
            ;;
        w)
            SANDBOX=$OPTARG
            ;;
        x)
            CLEANUP=$OPTARG
            ;;
        *)
            echo "Unknown option: $OPTION=$OPTARG"
            usage
            exit
            ;;
    esac
done

# Check that mandatory arguments are set
# (Currently all that are passed through to the agent)
if [[ -z $AUTH ]] ||\
   [[ -z $CORES ]] ||\
   [[ -z $DEBUG ]] ||\
   [[ -z $DBNAME ]] ||\
   [[ -z $DBURL ]] ||\
   [[ -z $LRMS ]] ||\
   [[ -z $MPI_LAUNCH_METHOD ]] ||\
   [[ -z $PILOTID ]] ||\
   [[ -z $RUNTIME ]] ||\
   [[ -z $SESSIONID ]] ||\
   [[ -z $TASK_LAUNCH_METHOD ]] ||\
   [[ -z $VERSION ]]; then
     echo "Missing option"
     usage
     exit 1
fi

# If the host that will run the agent is not capable of communication
# with the outside world directly, we will setup a tunnel.
if [[ $FORWARD_TUNNEL_ENDPOINT ]]; then

    echo ""
    echo "################################################################################"
    echo "## Setting up forward tunnel for MongoDB to $FORWARD_TUNNEL_ENDPOINT."

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
fi

# If PYTHON was not set as an argument, detect it here.
if [[ -z $PYTHON ]]; then
    PYTHON=`which python`
fi

# Reuse existing VE if specified
if [[ $GLOBAL_VIRTENV ]]; then

    VIRTENV=$GLOBAL_VIRTENV

    # activate the virtualenv
    source $VIRTENV/bin/activate
    
    # we never clean up global virtualenvs -- remove the 'v' cleanup flag
    CLEANUP=$(echo $CLEANUP | tr -d 'v')

    # this assumes that the VE lives outside of the pilot sandbox, which MUST be
    # true, as at the point where a global VE can be specified, the pilot UID is
    # still unknown.  That only conflicts if the pilot sandbox is specified
    # explicitly, and the global VE lives therein.  This case is, at this point,
    # ignored.

else
    # bootstrap virtualenv at default location
    VIRTENV=$SANDBOX/virtualenv/

    # create/update virtualenv.  This activates it.
    installvenv $VIRTENV
fi

# check if creation succeeded
if [[ ! -d $VIRTENV || ! -f $VIRTENV/bin/activate ]]; then
    echo "Virtual Environment at $VIRTENV not found, install or upgrade failed.  Continue anyways." 
    # in the rare case that everything is already installed in system space, we
    # actually don't need a virtualenv, and thus continue here.
fi

# Export the variables related to virtualenv,
# so that we can disable the virtualenv for the cu.
export _OLD_VIRTUAL_PATH
export _OLD_VIRTUAL_PYTHONHOME
export _OLD_VIRTUAL_PS1

# -----------------------------------------------------------------------------
# launch the radical agent
#
AGENT_CMD="python radical-pilot-agent.py\
    -a $AUTH\
    -b $BENCHMARK\
    -c $CORES\
    -d $DEBUG\
    -j $TASK_LAUNCH_METHOD\
    -k $MPI_LAUNCH_METHOD\
    -l $LRMS\
    -m $DBURL\
    -n $DBNAME\
    -p $PILOTID\
    -s $SESSIONID\
    -t $RUNTIME\
    -v $VERSION"

echo ""
echo "################################################################################"
echo "## Launching radical-pilot-agent for $CORES cores."
echo "## CMDLINE: $AGENT_CMD"
$AGENT_CMD
AGENT_EXITCODE=$?

# cleanup flags:
#   l : pilot log files
#   u : unit work dirs
#   v : virtualenv
#   e : everything
echo "CLEANUP: $CLEANUP"
contains $CLEANUP 'l' && echo "rm -r $SANDBOX/AGENT.*"
contains $CLEANUP 'u' && echo "rm -r $SANDBOX/unit-*"
contains $CLEANUP 'v' && echo "rm -r $VIRTENV/"
contains $CLEANUP 'e' && echo "rm -r $SANDBOX/"
# contains $CLEANUP 'l' && rm -r $SANDBOX/AGENT.*
# contains $CLEANUP 'u' && rm -r $SANDBOX/unit-*
# contains $CLEANUP 'v' && rm -r $VIRTENV/
# contains $CLEANUP 'e' && rm -r $SANDBOX/

# ... and exit
exit $AGENT_EXITCODE
