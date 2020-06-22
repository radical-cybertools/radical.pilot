#!/bin/sh

# The purpose of this code is to ensure a stable environment for RP tasks,
# which ideally is (as) identical (as possible) to the environment the pilot
# is originally placed in.  This script represents that state in the pilot's
# bootstrapper.  The first step is thus to capture that environment.
#
# Note that this capture will result in an unquoted dump where the values can
# contain spaces, quotes (double and single), non-printable characters etc.
# The guarantees we have are:
#
#   - variable names begin with a letter, and contain letters, numbers and
#     underscores.  From POSIX:
#
#       "Environment variable names used by the utilities in the Shell and
#        Utilities volume of IEEE Std 1003.1-2001 consist solely of uppercase
#        letters, digits, and the '_' (underscore) from the characters defined
#        in Portable Character Set and do not begin with a digit."
#
#     Note that implementations usually also support lowercase letters, so we'll
#     have to support that, too (event though it is rarely used for exported
#     system variables).
#
#   - variable values can have any character.  Again POSIX:
#
#       "For values to be portable across systems conforming to IEEE Std
#        1003.1-2001, the value shall be composed of characters from the
#        portable character set (except NUL [...])."
#
# So the rules for names are strict, for values they are, unfortunately, loose.
# Specifically, values can contain unprintable characters and also newlines.
#
# This code attempts to handle these cases: all lines which to not start with
# a valid variable name immediately followed by EOL or `=` are counted toward
# the previous line, separated by newlines.  We trust `env` to take care of
# correctly representing other nonprintable characters.
#
# Note that the original environment we capture here includes whatever the user
# sets in his .profile and other initialisation file.  This includes enviroment
# details the user considers useful for the workload eventual to run - but
# implies that agent and launch methods may need to reset some of those settings
# to obtain their own desired environment, for example via a `module reset` or
# similar.

# use RP_TEST as test case for an env setting the application needs to see
# unchanged
export RP_TEST=VIRGIN
export RP_TEST_VIRGIN=True

# capture env
env | sort > ./env.boot.1

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
  env | sort > $home/env.boot.2
) )


# only really keep whatever did not change between both captures
grep -Fxf ./env.boot.1 ./env.boot.2 | sort > ./env.boot
# rm   -f   ./env.boot.1 ./env.boot.2

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

