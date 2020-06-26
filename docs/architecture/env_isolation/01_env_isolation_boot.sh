#!/bin/sh

# This script represents that state in the pilot's bootstrapper.
# The first step is thus to capture that environment.
#
#
# Note that the original environment we capture here includes whatever the user
# sets in his .profile and other initialisation file.  This includes enviroment
# details the user considers useful for the workload eventual to run - but
# implies that agent and launch methods may need to reset some of those settings
# to obtain their own desired environment, for example via a `module reset` or
# similar.

# test case for an env setting the application needs to see
export RP_TEST=VIRGIN
export RP_TEST_VIRGIN=True

# capture env
env | sort > ./env.boot.1.env

# There are parts of the environment which are expected to change with every
# command: current PID, CWD, last return value, etc.  We can blacklist some of
# those, but we can also find out by running a couple of commands, compare the
# resulting environment to the original one: all variables which changed that
# way are not considered constant and will not be considered part of the
# original environment, i.e., will be ignored.

# add env noise in a sub-sub shell
home=$(pwd)
( (
  cd /
  cd /tmp
  true &
  false
  # capture env again
  env | sort > $home/env.boot.2.env
) )


# only really keep whatever did not change between both captures
# (we could also trigger additional blacklisting here)
grep -Fxf ./env.boot.1.env ./env.boot.2.env | sort > ./env.boot.env
# rm -f   ./env.boot.1.env ./env.boot.2.env

# preparation of the pilot env goes here, as usual
python3 -m venv ve3       2>&1 > /dev/null
. ve3/bin/activate
pip install radical.pilot 2>&1 > /dev/null

# the bootstrapper changed RC_TEST and sets it's own variables which should
# disappear later, both of which need to be reset before the application
# launches.
export RP_TEST=BOOT
export RP_TEST_BOOT=True

# Use `exec` to start the agent process.  The real RP agent may go through
# through a bootstrap chain to spawn sub-agents on compute nodes, but that makes
# no difference for our discussion.
exec ./02_env_isolation_agent.py


# ------------------------------------------------------------------------------

