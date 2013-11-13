#!/bin/bash

# -----------------------------------------------------------------------------
# Author: Ole Weidner (ole.weidner@rutgers.edu)
# Copyright 2013, Ole Weidner
# License under the MIT License
#
# This script launches a radical compute pilot agent.
#

# -----------------------------------------------------------------------------
# global variables
#
REMOTE=
UID=
WORKDIR=`pwd`
PYTHON=`which python`

# -----------------------------------------------------------------------------
# print out script usage help
#
usage()
{
cat << EOF
usage: $0 options

This script launches a radical compute pilot agent.

OPTIONS:
   -r      Address and port of the coordination service host (MongoDB)

   -u      The unique identifier (uid) of the pilot

   -d      The working (base) directory of the agent
           (default is '.')

   -p      The Python interpreter to use, e.g., python2.6
           (default is '/usr/bin/python')

   -h      Show this message

EOF
}

# -----------------------------------------------------------------------------
# create to working directory structure
#
makeworkdir()
{
R_BASE_DIR=$WORKDIR/.radical/
if [ ! -d $R_BASE_DIR ] 
then
    echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Creating base directory: $R_BASE_DIR"
    mkdir -p $R_BASE_DIR
fi
}

# -----------------------------------------------------------------------------
# bootstrap virtualenv - we always use the latest version from GitHub
#
installvenv()
{
R_SYS_DIR=$WORKDIR/.radical/sys/
# remove any old versionsion
if [ -d $R_SYS_DIR ] 
then
    echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Removing previous virtualenv: $R_SYS_DIR"
    rm -r $R_SYS_DIR
fi
# create a fresh virtualenv
echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Bootstraping a fresh virtualenv from https://raw.github.com/pypa/virtualenv"
curl --insecure -s https://raw.github.com/pypa/virtualenv/1.9.X/virtualenv.py | $PYTHON - --python=$PYTHON $R_SYS_DIR
source $R_SYS_DIR/bin/activate
echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Installing Python packages: saga-python, requests"
pip install saga-python
}

# -----------------------------------------------------------------------------
# launch the radical agent 
#
launchagent()
{
echo "hello there"
#python ./radical-agent.py -r $REMOTE -t OTPTOKEN
}

# -----------------------------------------------------------------------------
# MAIN 
#
# parse command line arguments
while getopts “hr:t:d:p” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         r)
             REMOTE=$OPTARG
             ;;
         u)
             UID=$OPTARG
             ;;
         d)
             WORKDIR=$OPTARG
             ;;
         p)
             PYTHON=$OPTARG
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

if [[ -z $REMOTE ]] || [[ -z $UID ]]
then
     usage
     exit 1
fi

echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Starting RHYTHMOS environment setup."
# create working direcotries
makeworkdir
# bootstrap virtualenv
installvenv
echo "`date +"%m-%d-%Y %T"` - [run-radical-agent.sh] (INFO) - Completed RHYTHMOS environment setup. "
launchagent

