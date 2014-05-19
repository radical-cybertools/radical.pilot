#!/bin/bash -l

# -----------------------------------------------------------------------------
# Author: Ole Weidner (ole.weidner@rutgers.edu)
# Copyright 2013-2014, radical@rutgers.edu
# License under the MIT License
#
# This script launches a radical.pilot compute pilot.
#

# -----------------------------------------------------------------------------
# global variables
#
VERSION=
PREBOOTSTRAP=
CLEANUP=
REMOTE=
CORES=
RUNTIME=
DBNAME=
PILOTID=
UNITMANAGERID=
SESSIONID=
WORKDIR=`pwd`
PYTHON=`which python`
LAUNCH_MODE=SSH
QUEUE=
ALLOCATION=

# -----------------------------------------------------------------------------
# print out script usage help
#
usage()
{
cat << EOF
usage: $0 options

This script launches a RADICAL-Pilot agent.

OPTIONS:
   -r      Address and port of the coordination service host (MongoDB)

   -d      The name of the database 

   -s      The unique identifier (uid) of the session

   -p      The unique identifier (uid) of the pilot

   -w      The working (base) directory of the pilot
           (default is '.')

   -i      The Python interpreter to use, e.g., python2.6
           (default is '/usr/bin/python')

   -l      Task lauch mode, AUTO, LOCAL, MPIRUN, APRUN

   -e      List of commands to run before botstrapping

   -t      Runtime in minutes

   -c      Number of requested cores

   -q      The name of the queue to use

   -a      The name of project / allocation to charge

   -C      Cleanup - delete virtualenv after execution

   -V      Version - the RADICAL-Pilot package version

   -h      Show this message

EOF
}

# -----------------------------------------------------------------------------
# bootstrap virtualenv - we always use the latest version from GitHub
#
installvenv()
{
R_SYS_DIR=$WORKDIR/virtualenv/
# remove any old versionsion
if [ -d $R_SYS_DIR ] 
then
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
curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't download virtuelenv via curl! ABORTING"
   exit 1
fi

tar xvfz virtualenv-1.9.tar.gz
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't unpack virtualenv! ABORTING"
   exit 1
fi

BOOTSTRAP_CMD="$PYTHON virtualenv-1.9/virtualenv.py --python=$PYTHON $R_SYS_DIR"
echo ""
echo "################################################################################"
echo "## Creating virtualenv"
echo "## CMDLINE: $BOOTSTRAP_CMD"
$BOOTSTRAP_CMD
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't bootstrap virtuelenv! ABORTING"
   exit 1
fi

# active the virtualenv
source $R_SYS_DIR/bin/activate

UPDATE_SETUPTOOLS_CMD="pip install --upgrade setuptools"
echo ""
echo "################################################################################"
echo "## Updating virtualenv"
echo "## CMDLINE: $UPDATE_SETUPTOOLS_CMD"
$UPDATE_SETUPTOOLS_CMD
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't update virtualenv! ABORTING"
   exit 1
fi

# for now we use development version directly from git
# this blows up the virtualenv (and agent bootstrap time) significantly
# at some point we will bootstrap everything directly from PyPi.
# maybe a flag would be good to switch between 'production' and 'deve' ? 
PIP_CMD="pip install python-hostlist"
echo ""
echo "################################################################################"
echo "## Installing python-hostlist"
echo "## CMDLINE: $PIP_CMD"
$PIP_CMD
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't install python-hostlist! ABORTING"
   exit 1
fi

PIP_CMD="pip install pymongo"
echo ""
echo "################################################################################"
echo "## Installing pymongo"
echo "## CMDLINE: $PIP_CMD"
$PIP_CMD
OUT=$?
if [ $OUT -ne 0 ];then
   echo "Couldn't install pymongo! ABORTING"
   exit 1
fi
}

# -----------------------------------------------------------------------------
# launch the radical agent 
#
launchagent()
{
AGENT_CMD="python radical-pilot-agent.py -d mongodb://$REMOTE -n $DBNAME -s $SESSIONID -p $PILOTID -c $CORES -t $RUNTIME -l $LAUNCH_MODE -V $VERSION"
echo ""
echo "################################################################################"
echo "## Launching radical-pilot-agent for $CORES cores."
echo "## CMDLINE: $AGENT_CMD"
           python radical-pilot-agent.py -d mongodb://$REMOTE -n $DBNAME -s $SESSIONID -p $PILOTID -c $CORES -t $RUNTIME -l $LAUNCH_MODE -V $VERSION
}

# -----------------------------------------------------------------------------
# MAIN 
#
# parse command line arguments
while getopts “hr:d:s:p:w:i:l:e:t:c:q:a:V:C” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         r)
             REMOTE=$OPTARG
             ;;
         d)
             DBNAME=$OPTARG
             ;;
         s)
             SESSIONID=$OPTARG
             ;;
         p)
             PILOTID=$OPTARG
             ;;
         w)
             WORKDIR=$OPTARG
             ;;
         i)
             PYTHON=$OPTARG
             ;;
         l)
             LAUNCH_MODE=$OPTARG
             ;;
         e)
             PREBOOTSTRAP=$OPTARG
             echo ""
             echo "################################################################################"
             echo "## Running pre-bootstrapping command"
             echo "## CMDLINE: $PREBOOTSTRAP"
             $PREBOOTSTRAP
             OUT=$?
             if [ $OUT -ne 0 ];then
                echo "Error running pre-boostrapping command! ABORTING"
                exit 1
             fi
             ;;
         t)
             RUNTIME=$OPTARG
             ;;
         c)
             CORES=$OPTARG
             ;;
         q)
             QUEUE=$OPTARG
             ;;
         a)
             ALLOCATION=$OPTARG
             ;;
         C)
             CLEANUP=true
             ;;
         V)  
             VERSION=$OPTARG
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

if [[ -z $REMOTE ]] || [[ -z $SESSIONID ]] || [[ -z $PILOTID ]] || [[ -z $DBNAME ]] || [[ -z $RUNTIME ]] || [[ -z $CORES ]] || [[ -z $VERSION ]]
then
     usage
     exit 1
fi

# bootstrap virtualenv
installvenv

# launch the agent
launchagent

# cleanup
rm -rf $WORKDIR/virtualenv*
rm -rf bootstrap-and-run-agent
rm -rf radical-pilot-agent.py

if [[ $CLEANUP ]]
then
    # if cleanup is set, we delete all CU sandboxes !!
    rm -rf $WORKDIR/unit-*
fi

# ... and exit
exit 0

