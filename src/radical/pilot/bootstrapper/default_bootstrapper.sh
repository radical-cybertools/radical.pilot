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
CLEANUP=
CORES=
DBNAME=
DBURL=
DEBUG=
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
WORKDIR=`pwd`

# -----------------------------------------------------------------------------
# print out script usage help
#
usage()
{
cat << EOF > /dev/stderr
usage: $0 options

This script launches a RADICAL-Pilot agent.

OPTIONS:
   -a      The name of project / allocation to charge.

   -b      Enable benchmarks.

   -c      Number of requested cores.

   -d      Specify debug level.

   -e      List of commands to run before bootstrapping.

   -f      Tunnel endpoint for connection forwarding.

   -g      Global shared virtualenv, do not install anything.

   -h      Show this message.

   -i      The Python interpreter to use, e.g., python2.6.
           (default is '/usr/bin/python')

   -j      Task launch method.

   -k      MPI launch method.

   -l      Type of Local Resource Management System.

   -m      Address and port of the coordination service host (MongoDB).

   -n      The name of the database.

   -p      The unique identifier (uid) of the pilot.

   -s      The unique identifier (uid) of the session.

   -t      Runtime in minutes.

   -v      Version - the RADICAL-Pilot package version.

   -w      The working (base) directory of the pilot.
           (default is '.')

   -x      Cleanup - delete virtualenv after execution.

EOF
}

# -----------------------------------------------------------------------------
# bootstrap virtualenv - we always use the latest version from GitHub
#
installvenv()
{
R_SYS_DIR=$WORKDIR/virtualenv/
# remove any old versionsion
if [[ -d $R_SYS_DIR ]]; then
    echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Removing previous virtualenv: $R_SYS_DIR"
    rm -r $R_SYS_DIR
fi

# create a fresh virtualenv. we use and older 1.9.x version of 
# virtualenv as this seems to work more reliable than newer versions.
CURL_CMD="curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz"
echo ""
echo "################################################################################"
echo "## Downloading and installing virtualenv"
echo "## CMDLINE: $CURL_CMD"
$CURL_CMD
OUT=$?
if [[ $OUT != 0 ]]; then
   echo "Couldn't download virtuelenv via curl! ABORTING"
   exit 1
fi

tar xvfz virtualenv-1.9.tar.gz
OUT=$?
if [[ $OUT != 0 ]]; then
   echo "Couldn't unpack virtualenv! ABORTING"
   exit 1
fi

BOOTSTRAP_CMD="$PYTHON virtualenv-1.9/virtualenv.py $R_SYS_DIR"
echo ""
echo "################################################################################"
echo "## Creating virtualenv"
echo "## CMDLINE: $BOOTSTRAP_CMD"
$BOOTSTRAP_CMD
OUT=$?
if [[ $OUT != 0 ]]; then
   echo "Couldn't bootstrap virtualenv! ABORTING"
   exit 1
fi

# active the virtualenv
source $R_SYS_DIR/bin/activate

DOWNGRADE_PIP_CMD="easy_install pip==1.2.1"
echo ""
echo "################################################################################"
echo "## Downgrading pip to 1.2.1"
echo "## CMDLINE: $DOWNGRADE_PIP_CMD"
$DOWNGRADE_PIP_CMD
OUT=$?
if [[ $OUT != 0 ]]; then
   echo "Couldn't downgrade pip! ABORTING"
   exit 1
fi

#UPDATE_SETUPTOOLS_CMD="pip install --upgrade setuptools"
#echo ""
#echo "################################################################################"
#echo "## Updating virtualenv"
#echo "## CMDLINE: $UPDATE_SETUPTOOLS_CMD"
#$UPDATE_SETUPTOOLS_CMD
#OUT=$?
#if [ $OUT -ne 0 ]; then
#   echo "Couldn't update virtualenv! ABORTING"
#   exit 1
#fi

PIP_CMD="pip install python-hostlist"
EASY_INSTALL_CMD="easy_install python-hostlist"
echo ""
echo "################################################################################"
echo "## Installing python-hostlist"
echo "## CMDLINE: $PIP_CMD"
$PIP_CMD
OUT=$?
if [[ $OUT != 0 ]]; then
    echo "pip install failed, trying easy_install ..."
    $EASY_INSTALL_CMD
    OUT=$?
    if [[ $OUT != 0 ]]; then
        echo "Easy install failed too, couldn't install python-hostlist! ABORTING"
        exit 1
    fi
fi

PIP_CMD="pip install pymongo"
EASY_INSTALL_CMD="easy_install pymongo"
echo ""
echo "################################################################################"
echo "## Installing pymongo"
echo "## CMDLINE: $PIP_CMD"
$PIP_CMD
OUT=$?
if [[ $OUT != 0 ]]; then
    echo "pip install failed, trying easy_install ..."
    $EASY_INSTALL_CMD
    OUT=$?
    if [[ $OUT != 0 ]]; then
        echo "Easy install failed too, couldn't install pymongo! ABORTING"
        exit 1
    fi
fi
}

# -----------------------------------------------------------------------------
# Find available port on the remote host where we can bind to
#
function find_available_port()
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

# Print environment, useful for debugging
echo ""
echo "################################################################################"
echo "## Environment of bootstrapper process:"
printenv

# parse command line arguments
BENCHMARK=0
while getopts "abc:d:e:f:g:hi:j:k:l:m:n:op:qrs:t:uv:w:xyz" OPTION; do
    case $OPTION in
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
            OUT=$?
            if [[ $OUT -ne 0 ]]; then
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
        v)
            # Passed to agent
            VERSION=$OPTARG
            ;;
        w)
            WORKDIR=$OPTARG
            ;;
        x)
            CLEANUP=true
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
if [[ -z $CORES ]] ||\
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
    ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:${DBURL%/} $FORWARD_TUNNEL_ENDPOINT &

    # Kill ssh process when bootstrapper dies, to prevent lingering ssh's
    trap 'jobs -p | xargs kill' EXIT

    # Overwrite DBURL
    DBURL=$BIND_ADDRESS:$DBPORT
fi

# If PYTHON was not set as an argument, detect it here.
if [[ -z $PYTHON ]]; then
    PYTHON=`which python`
fi

# Reuse existing VE if it exists
if [[ $GLOBAL_VIRTENV ]]; then
    if [[ ! -d $GLOBAL_VIRTENV || ! -f $GLOBAL_VIRTENV/bin/activate ]]; then
        echo "Global Virtual Environment not found!"
        exit 1
    fi
    source $GLOBAL_VIRTENV/bin/activate
else
    # bootstrap virtualenv
    installvenv
fi

# -----------------------------------------------------------------------------
# launch the radical agent
#
AGENT_CMD="python radical-pilot-agent.py\
    -b $BENCHMARK\
    -c $CORES\
    -d $DEBUG\
    -j $TASK_LAUNCH_METHOD\
    -k $MPI_LAUNCH_METHOD\
    -l $LRMS\
    -m mongodb://$DBURL\
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

# cleanup
rm -rf $WORKDIR/virtualenv*

if [[ $CLEANUP ]]; then
    # if cleanup is set, we delete all CU sandboxes !!
    rm -rf $WORKDIR/unit-*
fi

# ... and exit
exit $AGENT_EXITCODE
