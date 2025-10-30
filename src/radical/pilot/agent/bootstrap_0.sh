#!/bin/sh
# combine stdout and stderr
exec 2>&1

# explicitly cd to sandbox it to shield against bashrc shenanigans
test -z "$RP_PILOT_SANDBOX" || cd "$RP_PILOT_SANDBOX"

# Unset functions/aliases of commands that will be used during bootstrap as
# these custom functions can break assumed/expected behavior
export PS1='#'
export LC_NUMERIC="C"

unset PROMPT_COMMAND
unset -f cd ls uname pwd date bc cat echo grep

# we should always find the RU env helper in cwd or path
test -f ./radical-utils-env.sh && . ./radical-utils-env.sh
test -f ./radical-utils-env.sh || .   radical-utils-env.sh

# get out of any virtual or conda env (from .bashrc etc)
env_deactivate

# store the sorted env for logging, but also so that we can dig original env
# settings for task environments, if needed
mkdir -p env
env_dump -t env/bs0_orig.env


# Report where we are, as this is not always what you expect ;-)
# Save environment, useful for debugging
echo "# -------------------------------------------------------------------"
echo "bootstrap_0 running on host: `hostname -f`."
echo "bootstrap_0 started as     : '$0 $@'"
echo "safe environment of bootstrap_0"

# interleave stdout and stderr, to get a coherent set of log messages
if test -z "$RP_BOOTSTRAP_0_REDIR"
then
    echo "bootstrap_0 stderr redirected to stdout"
    export RP_BOOTSTRAP_0_REDIR=True
    exec 2>&1
fi

if test "`uname`" = 'Darwin'
then
    echo 'Darwin: increasing open file limit'
    ulimit -n 512
fi

# trap 'echo TRAP QUIT' QUIT
# trap 'echo TRAP EXIT' EXIT
# trap 'echo TRAP KILL' KILL
# trap 'echo TRAP TERM' TERM
# trap 'echo TRAP INT'  INT

# ------------------------------------------------------------------------------
# Copyright 2013-2015, RADICAL @ Rutgers
# Licensed under the MIT License
#
# This script launches a radical.pilot pilot.  If needed, it creates and
# populates a virtualenv on the fly, into $VIRTENV.
#
# https://xkcd.com/1987/
#
# Arguments passed to bootstrap_0 should be required by bootstrap_0 itself,
# and *not* be passed down to the agent.  Configuration used by the agent should
# go in the agent config file, and *not( be passed as an argument to
# bootstrap_0.  Only parameters used by both should be passed to the bootstrap_0
# and  consecutively passed to the agent. It is rarely justified to duplicate
# information as parameters and agent config entries.  Exceptions would be:
# 1) the shell scripts can't (easily) read from MongoDB, so they need to
#    to get the information as arguments;
# 2) the agent needs information that goes beyond what can be put in
#    arguments, both qualitative and quantitatively.
#
# ------------------------------------------------------------------------------
# global variables
#
TUNNEL_BIND_DEVICE="lo"
CLEANUP=
HOSTPORT=
RUNTIME=
VIRTENV=
VIRTENV_MODE=
LAUNCHER=
PILOT_ID=
RP_VERSION=
PYTHON=
PYTHON_DIST=
SESSION_ID=
SESSION_SANDBOX=
PILOT_SANDBOX=`pwd`
PREBOOTSTRAP2=""

# NOTE:  $HOME is set to the job sandbox on OSG.  Bah!
# FIXME: the need for this needs to be reconfirmed and documented
# mkdir -p .ssh/

# seconds to wait for lock files
# 10 min should be enough for anybody to create/update a virtenv...
LOCK_TIMEOUT=600 # 10 min

VIRTENV_VER="virtualenv-16.7.12"
VIRTENV_DIR="$VIRTENV_VER"
VIRTENV_TGZ="$VIRTENV_VER.tar.gz"
VIRTENV_TGZ_URL="https://files.pythonhosted.org/packages/1c/c2/7516ea983fc37cec2128e7cb0b2b516125a478f8fc633b8f5dfa849f13f7/$VIRTENV_TGZ"
VIRTENV_IS_ACTIVATED=FALSE

echo $VIRTENV_TGZ_URL


# ------------------------------------------------------------------------------
#
# disable user site packages as those can conflict with our virtualenv
# installation -- see https://github.com/conda/conda/issues/448
#
# NOTE: we need to make sure this is inherited into sub-agent shells
#
export PYTHONNOUSERSITE=True

# we someetimes need to install modules via pip, and some need to be compiled
# during installation.  To speed this up (specifically on cluster compute
# nodes), we try to convince PIP to run parallel `make`
export MAKEFLAGS="-j"


# ------------------------------------------------------------------------------
#
# check what env variables changed from the original env, and create a
# `deactivate` script which resets the original values.
#
create_deactivate()
{
    profile_event 'create_deactivate_start'

    # at this point we activated the agent VE and thus have RU availale.  Use
    # `radical-utils-env.sh` to create a script to recover the virgin env
    # (`bs0_orig.env`) from the current state (`bs0_acivate.env`)
    profile_event 'create_deactivate_start_1'
    which radical-utils-env.sh
    profile_event 'create_deactivate_start_2'
    . radical-utils-env.sh
    profile_event 'create_deactivate_start_3'
    env_dump -t env/bs0_active.env
    profile_event 'create_deactivate_start_4'
    env_prep -s env/bs0_orig.env -r env/bs0_active.env -t env/bs0_orig.sh
    profile_event 'create_deactivate_start_5'

    # we also prepare a script to return to the `pre_bootstrap_0` state: all
    # pre_bootstrap_0 commands have been run, but the virtualenv is not yet
    # activated.  This can be used in case a different VE is to be created or
    # used.
    env_prep -s env/bs0_pre_0.env -r env/bs0_active.env -t env/bs0_pre_0.sh

    profile_event 'create_deactivate_stop'
}


# ------------------------------------------------------------------------------
#
# If profiling is enabled, compile our little gtod app and take the first time
#
create_gtod()
{
    profile_event 'gtod_start'

    tmp=`date '+%s.%N' | grep N`
    if test "$?" = 0
    then
        if ! contains "$tmp" '%'
        then
            # we can use the system tool
            echo "#!/bin/sh"       > ./gtod.sh
            echo "date '+%s.%6N'" >> ./gtod.sh
        fi
    else
        shell=/bin/sh
        test -x '/bin/bash' && shell=/bin/bash

        echo "#!$SHELL"                                > ./gtod.sh
        echo "export LC_NUMERIC=C"                    >> ./gtod.sh
        echo "if test -z \"\$EPOCHREALTIME\""         >> ./gtod.sh
        echo "then"                                   >> ./gtod.sh
        echo "  awk 'BEGIN {srand(); print srand()}'" >> ./gtod.sh
        echo "else"                                   >> ./gtod.sh
        echo "  echo \${EPOCHREALTIME:0:20}"          >> ./gtod.sh
        echo "fi"                                     >> ./gtod.sh
    fi

    chmod 0755 ./gtod.sh

    # initialize profile
    PROFILE="bootstrap_0.prof"
    now=$(./gtod.sh)
    echo "#time,event,comp,thread,uid,state,msg" > "$PROFILE"

    ip=$(ip addr \
         | grep 'state UP' -A2 \
         | grep 'inet' \
         | awk '{print $2}' \
         | cut -f1 -d'/' \
         | tr '\n' ' ' \
         | cut -f1 -d' ')
    printf "%.6f,%s,%s,%s,%s,%s,%s\n" \
        "$now" "sync_abs" "bootstrap_0" "MainThread" "$PILOT_ID" \
        "$pilot_state" "$(hostname):$ip:$now:$now:$now" \
        | tee -a "$PROFILE"

    profile_event 'gtod_stop'
}


# ------------------------------------------------------------------------------
#
create_prof(){

    cat > ./prof <<EOT
#!/bin/sh

test -z "\$RP_PROF_TGT" && exit

now=\$(\$RP_GTOD)
printf "%.7f,\$1,\$RP_SPAWNER_ID,MainThread,\$RP_TASK_ID,AGENT_EXECUTING,\$2\\\n" \$now\\
    >> "\$RP_PROF_TGT"

EOT

    chmod 0755 ./prof
}


# ------------------------------------------------------------------------------
#
profile_event()
{
    if test -z "$RADICAL_PILOT_PROFILE$RADICAL_PROFILE"
    then
        return
    fi

    PROFILE="bootstrap_0.prof"

    event=$1
    msg=$2
    now=$(./gtod.sh)

    # TIME   = 0  # time of event (float, seconds since epoch)  mandatory
    # EVENT  = 1  # event ID (string)                           mandatory
    # COMP   = 2  # component which recorded the event          mandatory
    # TID    = 3  # uid of thread involved                      optional
    # UID    = 4  # uid of entity involved                      optional
    # STATE  = 5  # state of entity involved                    optional
    # MSG    = 6  # message describing the event                optional
    # ENTITY = 7  # type of entity involved                     optional
    printf "%.6f,%s,%s,%s,%s,%s,%s\n" \
        "$now" "$event" "bootstrap_0" "MainThread" "$PILOT_ID" "$pilot_state" "$msg" \
        >> "$PROFILE"
}

last_event()
{
    profile_event 'bootstrap_0_stop'
}

# make sure we captur last event
trap last_event INT TERM



# ------------------------------------------------------------------------------
#
# we add another safety feature to ensure agent cancelation after runtime
# expires: the timeout() function expects *exactly* two processes to run in the
# background.  Whichever finishes with will cause a SIGUSR1 signal, which is
# then trapped to kill both processes.  Since the first one is dead, only the
# second will actually get the kill, and the subsequent wait will thus succeed.
# The second process is, of course, a `sleep $TIMEOUT`, so that the actual
# workload process will get killed after that timeout...
#
timeout()
{
    profile_event 'timeout_start'

    TIMEOUT="$1"; shift
    COMMAND="$*"

    RET="./timetrap.$$.ret"

    # note that this insane construct uses `$PID_1` and `$PID_2` which will
    # only be set later on.  In fact, those may or may not be set at all...
    timetrap()
    {
        kill $PID_1 2>&1 > /dev/null
        kill $PID_2 2>&1 > /dev/null
    }
    trap timetrap USR1

    rm -f $RET
    ($COMMAND;       echo "$?" >> $RET; /bin/kill -s USR1 $$) & PID_1=$!
    (sleep $TIMEOUT; echo "1"  >> $RET; /bin/kill -s USR1 $$) & PID_2=$!

    wait

    ret=`cat $RET || echo 2`
    rm -f $RET

    profile_event 'timeout_stop'

    return $ret
}


# ------------------------------------------------------------------------------
#
# a similar method is `waitfor()`, which will test a condition in certain
# intervals and return once that condition is met, or finish after a timeout.
# Other than `timeout()` above, this method will not create subshells, and thus
# can be utilized for job control etc.
#
waitfor()
{
    profile_event 'waitfor_start'

    INTERVAL="$1"; shift
    TIMEOUT="$1";  shift
    COMMAND="$*"

    START=`echo \`./gtod.sh\` | cut -f 1 -d .`
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
        NOW=`echo \`./gtod.sh\` | cut -f 1 -d .`
    done

    if test "$RET" = 0
    then
        echo "COND timeout"
    fi

    profile_event 'waitfor_stop'

    return $RET
}


# ------------------------------------------------------------------------------
#
# some virtenv operations need to be protected against pilots starting up
# concurrently, so we lock the virtualenv directory during creation and update.
#
# I/O redirect under noclobber is atomic in POSIX
#
lock()
{
    profile_event 'lock_start'

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

    err=`/bin/bash -c "set -C ; echo $pid > '$lockfile' && chmod a+r '$lockfile' && echo ok" 2>&1`
    until test "$err" = "ok"
    do
        if contains "$err" 'no such file or directory'
        then
            # there is something wrong with the lockfile path...
            echo "ERROR: can't create lockfile at '$lockfile' - invalid directory?"
            exit 1
        fi

        owner=`cat $lockfile 2>/dev/null`
        count=$((count+1))

        echo "wait for lock $lockfile (owned by $owner) $((timeout-count))"

        if test $count -gt $timeout
        then
            echo "### WARNING ###"
            echo "lock timeout for $entry -- removing stale lock for '$owner'"
            rm $lockfile
            # we do not exit the loop here, but race again against other pilots
            # waiting for this lock.
            count=0
        else

            # need to wait longer for lock release
            sleep 1
        fi

        # retry
        cmd="set -C ; echo $pid > $lockfile && chmod a+r $lockfile && echo ok"
        err=`/bin/bash -c "$cmd" 2>&1`
    done

    # one way or the other, we got the lock finally.
    echo "obtained lock $lockfile"

    profile_event 'lock_stop'
}


# ------------------------------------------------------------------------------
#
# remove an previously qcquired lock.  This will abort if the lock is already
# gone, or if it is not owned by us -- both cases indicate that a different
# pilot got tired of waiting for us and forcefully took over the lock
#
unlock()
{
    profile_event 'unlock_start'

    pid="$1"      # ID of pilot/bootstrapper which has the lock
    entry="$2"    # locked entry

    # clean $entry (normalize path, remove trailing slash, etc
    entry="`dirname $entry`/`basename $entry`"

    lockfile="$entry.lock"

    if ! test -f $lockfile
    then
        echo "ERROR: can't unlock $entry for $pid: missing lock $lockfile"
        exit 1
    fi

    owner=`cat $lockfile`
    if ! test "$owner" = "`echo $pid`"
    then
        echo "ERROR: can't unlock $entry for $pid: owner is $owner"
        exit 1
    fi

    rm -vf $lockfile

    profile_event 'unlock_stop'
}


# ------------------------------------------------------------------------------
#
# after installing and updating pip, and after activating a VE, we want to make
# sure we use the correct python and pip executables.  This rehash sets $PIP and
# $PYTHON to the respective values.  Those variables should be used throughout
# the code, to avoid any ambiguity due to $PATH, aliases and shell functions.
#
# The only argument is optional, and can be used to pin a specific python
# executable.
#
rehash()
{
    profile_event 'rehash_start'

    explicit_python="$1"

    # If PYTHON was not set as an argument, detect it here.
    # we need to do this again after the virtenv is loaded
    if test -z "$explicit_python"
    then
        PYTHON=`which python3`
    else
        PYTHON="$explicit_python"
    fi

    if test -z "$PYTHON"
    then
        echo "ERROR: no python"
        env_dump no_python.env
        exit 1
    fi

    code='from sys import version_info as vi; print(f"{vi.major}.{vi.minor}")'
    PYTHON_VERSION=$($PYTHON -c "$code")

    # NOTE: if a cacert.pem.gz was staged, we unpack it and use it for all pip
    #       commands (It means that the pip cacert [or the system's, dunno]
    #       is not up to date).  Easy_install seems to use a different access
    #       channel for some reason, so does not need the cert bundle.
    #       see https://github.com/pypa/pip/issues/2130
    #       ca-cert bundle from http://curl.haxx.se/docs/caextract.html

    # NOTE: Condor does not support staging into some arbitrary
    #       directory, so we may find the dists in pwd
    CA_CERT_GZ="$SESSION_SANDBOX/cacert.pem.gz"
    CA_CERT_PEM="$SESSION_SANDBOX/cacert.pem"
    if ! test -f "$CA_CERT_GZ" -o -f "$CA_CERT_PEM"
    then
        CA_CERT_GZ="./cacert.pem.gz"
        CA_CERT_PEM="./cacert.pem"
    fi

    if test -f "$CA_CERT_GZ"
    then
        gunzip "$CA_CERT_GZ"
    fi

    PIP="$PYTHON -m pip"
    if test -f "$CA_CERT_PEM"
    then
        PIP="$PIP --cert $CA_CERT_PEM"
    fi

    # NOTE: some resources define a function pip() to implement the same cacert
    #       fix we do above.  On some machines, that is broken (hello archer),
    #       thus we undefine that function here.
    unset -f pip

    echo "PYTHON            : $PYTHON"
    echo "PYTHON_VERSION    : $PYTHON_VERSION"
    echo "PIP               : $PIP"
    echo "PIP version       : `$PIP --version`"

    profile_event 'rehash_stop'
}


# ------------------------------------------------------------------------------
# verify that we have a usable python installation
verify_install()
{
    profile_event 'verify_install_start'

    verify_rp_install
    echo    "PYTHONPATH             : $PYTHONPATH"
    echo -n "Verify python viability: $PYTHON ..."
    if ! $PYTHON -c 'import sys; assert sys.version_info >= (3,5)'
    then
        echo ' failed'
        echo "python installation ($PYTHON) is not usable - abort"
        exit 1
    fi
    echo ' ok'

    profile_event 'verify_install_stop'
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

    profile_event 'run_cmd_start' "$msg"

    echo ""
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# $msg"
    echo "# cmd: $cmd"
    echo "#"
    eval "$cmd" 2>&1
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
            eval "$fallback"
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

    profile_event 'run_cmd_stop'
}



# ------------------------------------------------------------------------------
#
# create and/or update a virtenv, depending on mode specifier:
#
#   'private' : error  if it exists, otherwise create, then use
#   'update'  : update if it exists, otherwise create, then use
#   'create'  : use    if it exists, otherwise create, then use
#   'use'     : use    if it exists, otherwise error,  then exit
#   'recreate': delete if it exists, otherwise create, then use
#
# create and update ops will be locked and thus protected against concurrent
# bootstrap_0 invocations.
#
# (private + location in pilot sandbox == old behavior)
#
# That locking will likely not scale nicely for larger numbers of concurrent
# pilots, at least not for slow running updates (time for update of n pilots
# needs to be smaller than lock timeout).  OTOH, concurrent pip updates should
# not have a negative impact on the virtenv in the first place, AFAIU -- lock on
# create is more important, and should be less critical
#
virtenv_setup()
{
    profile_event 'virtenv_setup_start'

    pid="$1"
    virtenv="$2"
    virtenv_mode="$3"
    python_dist="$4"

    ve_create=UNDEFINED
    ve_update=UNDEFINED

    case "$virtenv_mode" in
        local)
            if test "$RP_VERSION" = 'local'; then
                printf "WARNING: use installed RP for VE_MODE=local\n"
                export RP_VERSION='installed'
            fi
            if ! test -d "$virtenv/"; then
                printf "\nERROR: given virtenv does not exist at $virtenv\n\n"
                exit 1
            fi
            ve_create=FALSE
            ve_update=FALSE
            ;;

        none)
            if test "$RP_VERSION" = 'local'; then
                printf "WARNING: use installed RP for VE_MODE=none\n"
                export RP_VERSION='installed'
            fi
            virtenv=''
            ve_create=FALSE
            ve_update=FALSE
            ;;

        private)
            if test -d "$virtenv/"; then
                printf "\nERROR: private virtenv already exists at $virtenv\n\n"
                exit 1
            fi
            ve_create=TRUE
            ve_update=FALSE
            ;;

        update)
            ve_create=FALSE
            ve_update=TRUE
            test -d "$virtenv/" || ve_create=TRUE
            ;;

        create)
            ve_create=TRUE
            ve_update=FALSE
            ;;

        use)
            if ! test -d "$virtenv/"; then
                printf "\nERROR: given virtenv does not exist at $virtenv\n\n"
                exit 1
            fi
            ve_create=FALSE
            ve_update=FALSE
            ;;

        recreate)
            test -d "$virtenv/" && rm -r "$virtenv"
            ve_create=TRUE
            ve_update=FALSE
            ;;

        *)
            ve_create=FALSE
            ve_update=FALSE
            printf "\nERROR: virtenv mode invalid: $virtenv_mode\n\n"
            exit 1
            ;;
    esac


    if test "$ve_create" = 'TRUE'
    then
        # no need to update a fresh ve
        ve_update=FALSE
    fi

    echo "virtenv_create    : $ve_create"
    echo "virtenv_update    : $ve_update"


    # radical_pilot installation and update is governed by PILOT_VERSION.  If
    # that is set to 'stage', we install the release and use the pilot which was
    # staged to pwd.  If set to 'release', we install from pypi.  In all other
    # cases, we install from git at a specific tag or branch
    #
    case "$RP_VERSION" in

        release)
            RP_INSTALL_SOURCES='radical.pilot'
            RP_INSTALL_TARGET='SANDBOX'
            ;;

        installed)
            RP_INSTALL_SOURCES=''
            RP_INSTALL_TARGET=''
            ;;

        local)
            # FIXME
            RP_INSTALL_SOURCES='radical.pilot'
            RP_INSTALL_TARGET='SANDBOX'
            ;;

        *)
            # NOTE: do *not* use 'pip -e' -- egg linking does not work with
            #       PYTHONPATH.  Instead, we manually clone the respective
            #       git repository, and switch to the branch/tag/commit.
            RP_INSTALL_SOURCES="radical.pilot==$RP_VERSION"
            RP_INSTALL_TARGET='SANDBOX'
    esac

    # NOTE: for any immutable virtenv (VIRTENV_MODE==use), we have to choose
    #       a SANDBOX install target.
    if test "$virtenv_mode" = "use" \
       -o   "$virtenv_mode" = "local" \
       -o   "$virtenv_mode" = "none"
    then
        if test "$RP_INSTALL_TARGET" = "VIRTENV"
        then
            echo "WARNING: virtenv immutable - install RP locally"
            RP_INSTALL_TARGET='SANDBOX'
        fi
    fi

    # A ve lock is not needed (nor desired) on sandbox installs.
    RP_INSTALL_LOCK='FALSE'
    if test "$RP_INSTALL_TARGET" = "VIRTENV"
    then
        RP_INSTALL_LOCK='TRUE'
    fi

    echo "rp install sources: $RP_INSTALL_SOURCES"
    echo "rp install target : $RP_INSTALL_TARGET"
    echo "rp install lock   : $RP_INSTALL_LOCK"


    # create virtenv if needed.  This also activates the virtenv.
    if test "$ve_create" = "TRUE"
    then
        if ! test -d "$virtenv/"
        then
            echo 'rp lock for virtenv create'
            lock "$pid" "$virtenv" # use default timeout
            virtenv_create "$virtenv" "$python_dist"
            if ! test "$?" = 0
            then
                echo "ERROR: couldn't create virtenv - abort"
                unlock "$pid" "$virtenv"
                exit 1
            fi
            unlock "$pid" "$virtenv"
        else
            echo "virtenv $virtenv exists"
        fi
    else
        echo "do not create virtenv $virtenv"
    fi

    # creation or not -- at this point it needs activation
    virtenv_activate "$virtenv" "$python_dist"


    # update virtenv if needed.  This also activates the virtenv.
    if test "$ve_update" = "TRUE"
    then
        echo 'rp lock for virtenv update'
        lock "$pid" "$virtenv" # use default timeout
        virtenv_update "$virtenv" "$python_dist"
        if ! test "$?" = 0
        then
            echo "ERROR: couldn't update virtenv - abort"
            unlock "$pid" "$virtenv"
            exit 1
       fi
       unlock "$pid" "$virtenv"
    else
        echo "do not update virtenv $virtenv"
    fi

    virtenv_eval

    # install RP
    if test "$RP_INSTALL_LOCK" = 'TRUE'
    then
        echo "rp lock for rp install (target: $RP_INSTALL_TARGET)"
        lock "$pid" "$virtenv" # use default timeout
    fi
    rp_install "$RP_INSTALL_SOURCES" "$RP_INSTALL_TARGET"
    if test "$RP_INSTALL_LOCK" = 'TRUE'
    then
       unlock "$pid" "$virtenv"
    fi

    profile_event 'virtenv_setup_stop'
}


# ------------------------------------------------------------------------------
#
virtenv_activate()
{
    if test "$VIRTENV_IS_ACTIVATED" = "TRUE"; then
        return
    fi

    profile_event 'virtenv_activate_start'

    virtenv="$1"
    python_dist="$2"

    if test "$VIRTENV_MODE" = 'none'; then
        export RP_VENV_TYPE='shell'
        # no virtenv to activate
        true

    elif test "$python_dist" = "anaconda"; then
        export RP_VENV_TYPE='conda'
        if ! test -z "$(which conda)"; then
            eval "$(conda shell.posix hook)"
            conda activate "$virtenv"

        elif test -e "$virtenv/bin/activate"; then
            . "$virtenv/bin/activate"

        elif test -e "$virtenv/../../bin/activate"; then
            # check conda base directory
            . "$virtenv/../../bin/activate" "$(basename $virtenv)"
        fi

        if test -z "$CONDA_PREFIX"; then
            echo "Loading of conda env failed!"
            exit 1
        fi

    else
        export RP_VENV_TYPE='venv'
        unset VIRTUAL_ENV
        . "$virtenv/bin/activate"
        if test -z "$VIRTUAL_ENV"; then
            echo "Loading of virtual env failed!"
            exit 1
        fi
    fi


    export RP_VENV_PATH="$virtenv"

    VIRTENV_IS_ACTIVATED=TRUE
    echo "VIRTENV activated : $virtenv"

    if test "$VIRTENV_MODE" != 'none'; then
        RP_PATH="$virtenv/bin"
        echo "RP_PATH           : $RP_PATH"

        PATH="$RP_PATH:$PATH"
        export PATH
    fi

    profile_event 'virtenv_activate_stop'
}


# ------------------------------------------------------------------------------
#
virtenv_eval()
{

    profile_event 'virtenv_eval_start'
    # make sure we use the new python binary
    rehash

    # make sure the lib path into the prefix conforms to the python conventions
    VE_MOD_PREFIX=`$PYTHON -c 'import sys; print(sys.prefix)'`
    echo "VE_MOD_PREFIX init: $VE_MOD_PREFIX"
    # NOTE: distutils.sc.get_python_lib() behaves different on different
    #       systems: on some systems (versions?) it returns a normalized path,
    #       on some it does not.  As we need consistent behavior to have
    #       a chance of the sed below to succeed, we normalize the path ourself.
  # VE_MOD_PREFIX=`(cd $VE_MOD_PREFIX; pwd -P)`

    # NOTE: on other systems again, that above path normalization is resulting
    #       in paths which are invalid when used with pip/PYTHONPATH, as that
    #       will result in the incorrect use of .../lib/ vs. .../lib64/ (it is
    #       a symlink in the VE, but is created as distinct dir by pip).  So we
    #       have to perform the path normalization only on the part with points
    #       to the root of the VE: we don't apply the path normalization to
    #       the last three path elements (lib[64]/pythonx.y/site-packages) (this
    #       probably should be an sed command...)
    TMP_BASE="$VE_MOD_PREFIX/"
    TMP_TAIL="`basename $TMP_BASE`"
    TMP_BASE="`dirname  $TMP_BASE`"
    TMP_TAIL="`basename $TMP_BASE`/$TMP_TAIL"
    TMP_BASE="`dirname  $TMP_BASE`"
    TMP_TAIL="`basename $TMP_BASE`/$TMP_TAIL"
    TMP_BASE="`dirname  $TMP_BASE`"

    TMP_BASE=`(cd $TMP_BASE; pwd -P)`
    VE_MOD_PREFIX="$TMP_BASE/$TMP_TAIL"
    echo "VE_MOD_PREFIX     : $VE_MOD_PREFIX"

    # we can now derive the pythonpath into the rp_install portion by replacing
    # the leading path elements.  The same mechanism is used later on
    # to derive the PYTHONPATH into the sandbox rp_install, if needed.
    RP_MOD_PREFIX="$PILOT_SANDBOX/rp_install"
    echo "RP_MOD_PREFIX     : $RP_MOD_PREFIX"

    # NOTE: this should not be necessary, but we explicit set PYTHONPATH to
    #       include the VE module tree, because some systems set a PYTHONPATH on
    #       'module load python', and that would supersede the VE module tree,
    #       leading to unusable versions of setuptools.
    VE_PYTHONPATH="$PYTHONPATH"
    echo "VE_PYTHONPATH     : $VE_PYTHONPATH"

    PYTHONPATH="$VE_MOD_PREFIX:$VE_PYTHONPATH"
    export PYTHONPATH

    profile_event 'virtenv_eval_stop'
}


# ------------------------------------------------------------------------------
#
# create virtualenv - we always use the latest version from GitHub
#
# The virtenv creation will also install the required packges, but will (mostly)
# not use '--upgrade' for dependencies, so that will become a noop if the
# packages have been installed before.  An eventual upgrade will be triggered
# independently in virtenv_update().
#
virtenv_create()
{
    # create a fresh ve
    profile_event 'virtenv_create_start'

    virtenv="$1"
    python_dist="$2"

    if test "$python_dist" = "anaconda"
    then
        run_cmd "Create virtualenv" \
                "conda create -y -p $virtenv python=3.8"
        if test $? -ne 0
        then
            echo "ERROR: couldn't create (conda) virtualenv"
            return 1
        fi

    elif test "$python_dist" = "default"
    then

        # check if the `venv` module is usable
        ok=''
        if run_cmd "Create ve with venv" \
                   "python3 -m venv $virtenv"
        then
            ok='true'
        fi


        # check if virtualenv is available
        if ! test "$ok" = 'true'
        then

            if run_cmd "Create ve with system virtualenv" \
                       "virtualenv $virtenv"
            then
                ok='true'
            fi
        fi

        # fall back to local virtualenv deployment
        if ! test "$ok" = 'true'
        then

            flags='-1 -k -L -O'
            if (hostname -f | grep -e '^smic' > /dev/null)
            then
                flags='-k -L -O'
            fi

            run_cmd "Download virtualenv tgz" \
                    "curl $flags '$VIRTENV_TGZ_URL'"

            if ! test "$?" = 0
            then
                echo "ERROR: couldn't download virtualenv via curl"
                exit 1
            fi

            run_cmd "unpacking virtualenv tgz" \
                    "tar zxmf '$VIRTENV_TGZ'"

            if test $? -ne 0
            then
                echo "ERROR: Couldn't unpack virtualenv!"
                exit 1
            fi

            run_cmd "Create ve with virtualenv" \
                    "$PYTHON $VIRTENV_DIR/virtualenv.py $virtenv"

            if test $? -ne 0
            then
                echo "ERROR: couldn't create virtualenv"
                return 1
            fi
        fi

        # clean out virtenv sources
        if test -d "$VIRTENV_DIR"
        then
            rm -rf "$VIRTENV_DIR" "$VIRTENV_TGZ"
        fi

    else
        echo "ERROR: invalid python_dist option ($python_dist)"
        return 1
    fi


    # activate the virtualenv
    virtenv_activate "$virtenv" "$python_dist"

    # make sure we have pip
    if test -z "$(which pip)"
    then
        run_cmd "install pip" \
                "easy_install pip" \
             || echo "Couldn't install pip! Uh oh...."
    fi

    # update required base modules
    # of the RADICAL stack
    run_cmd "update  venv" \
            "pip --no-cache-dir install --upgrade pip setuptools wheel" \
         || echo "Couldn't update venv! Lets see how far we get ..."

    # make sure the new pip version is used
    rehash

    # collect ve info
    virtenv_eval

    profile_event 'virtenv_create_stop'
}


# ------------------------------------------------------------------------------
#
# update virtualenv - this assumes that the virtenv has been activated
#
virtenv_update()
{
    profile_event 'virtenv_update_start'

    virtenv="$1"
    python_dist="$2"

    # activate the virtualenv
    virtenv_activate "$virtenv" "$python_dist"
    virtenv_eval

    profile_event 'virtenv_update_stop'
}


# ------------------------------------------------------------------------------
#
# Install the radical stack, ie. install RP which pulls the rest.
# This assumes that the virtenv has been activated.  Any previously installed
# stack version is deleted.
#
# . $VIRTENV/bin/activate
# rm -rf $VIRTENV/rp_install
#
# case rp_version:
#
#   installed:
#       true
#
#   <version string>:
#       pip install -t $SANDBOX/rp_install radical.pilot
#       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
#       export PATH=$SANDBOX/rp_install/bin:$PATH
#
# esac
#
rp_install()
{
    profile_event 'rp_install_start'

    rp_install_sources="$1"
    rp_install_target="$2"

    if test -z "$rp_install_target"
    then
        echo "no RP install target - skip install"

        # we just activate the rp_install portion of the used virtenv
        PYTHONPATH="$RP_MOD_PREFIX:$VE_MOD_PREFIX:$VE_PYTHONPATH"
        export PYTHONPATH

        PATH="$VIRTENV/rp_install/bin:$PATH"
        export PATH

        return
    fi

    echo "Using RADICAL-Pilot install sources '$rp_install_sources'"

    # install rp into a separate tree -- no matter if in shared ve or a local
    # sandbox or elsewhere
    case "$rp_install_target" in

        VIRTENV)
            RP_INSTALL="$VIRTENV/rp_install"

            # no local install -- we want to install in the rp_install portion of
            # the ve.  The pythonpath is set to include that part.
            PYTHONPATH="$RP_MOD_PREFIX:$VE_MOD_PREFIX:$VE_PYTHONPATH"
            export PYTHONPATH

            PATH="$VIRTENV/rp_install/bin:$PATH"
            export PATH

            RADICAL_MOD_PREFIX="$RP_MOD_PREFIX/radical/"

            echo "using virtenv install tree"
            echo "PYTHONPATH        : $PYTHONPATH"
            echo "RP_MOD_PREFIX     : $RP_MOD_PREFIX"
            echo "RADICAL_MOD_PREFIX: $RADICAL_MOD_PREFIX"
            ;;

        SANDBOX)
            RP_INSTALL="$PILOT_SANDBOX/rp_install"

            # make sure the lib path into the prefix conforms to the python conventions
            # FIXME
            RP_LOC_PREFIX=`echo $VE_MOD_PREFIX | sed -e "s|$VIRTENV|$RP_INSTALL/|"`
            RP_LOC_PREFIX="$RP_INSTALL"

            echo "VE_MOD_PREFIX     : $VE_MOD_PREFIX"
            echo "VIRTENV           : $VIRTENV"
            echo "SANDBOX           : $PILOT_SANDBOX"
            echo "VE_LOC_PREFIX     : $VE_LOC_PREFIX"

            # local PYTHONPATH needs to be pre-pended.  The ve PYTHONPATH is
            # already set during ve activation -- but we don't want the rp_install
            # portion from that ve...
            # NOTE: PYTHONPATH is set differently than the 'prefix' used during
            #       install
            PYTHONPATH="$RP_LOC_PREFIX:$VE_MOD_REFIX:$VE_PYTHONPATH"
            export PYTHONPATH

            PATH="$PILOT_SANDBOX/rp_install/bin:$PATH"
            export PATH

            RADICAL_MOD_PREFIX="$RP_LOC_PREFIX/radical/"

            echo "using local install tree"
            echo "PYTHONPATH        : $PYTHONPATH"
            echo "RP_LOC_PREFIX     : $RP_LOC_PREFIX"
            echo "RADICAL_MOD_PREFIX: $RADICAL_MOD_PREFIX"
            ;;

        *)
            # this should never happen
            echo "ERROR: invalid RP install target '$RP_INSTALL_TARGET'"
            exit 1

    esac

    # NOTE: we need to purge the whole install tree (not only the module dir),
    #       as pip will otherwise find the eggs and interpret them as satisfied
    #       dependencies, even if the modules are gone.  Of course, there should
    #       not be any eggs in the first place, but...
    rm    -rf  "$RP_INSTALL/"
    mkdir -p   "$RP_INSTALL/"

    pip_flags="$pip_flags --target '$RP_INSTALL'"
    pip_flags="$pip_flags --no-cache-dir --no-build-isolation"

    for src in $rp_install_sources
    do
        run_cmd "update $src via pip" \
                "$PIP install $pip_flags $src"

        if test $? -ne 0
        then
            echo "Couldn't install $src! Lets see how far we get ..."
        fi
    done

    profile_event 'rp_install_stop'
}


# ------------------------------------------------------------------------------
# Verify that we ended up with a usable installation.  This will also print all
# versions and module locations, which is nice for debugging...
#
verify_rp_install()
{
    profile_event 'verify_rp_install_start'

    OLD_RADICAL_VERBOSE=$RADICAL_VERBOSE
    OLD_RADICAL_PILOT_VERBOSE=$RADICAL_PILOT_VERBOSE

    RADICAL_VERBOSE=WARNING
    RADICAL_PILOT_VERBOSE=WARNING

    # print the ve information and stack versions for verification
    echo
    echo "---------------------------------------------------------------------"
    echo
    echo "`$PYTHON --version` ($PYTHON)"
    echo "PYTHONPATH: $PYTHONPATH"
    $PYTHON $(which radical-stack) \
        || (echo 'install failed!'; false) \
        || exit 1
    echo
    echo "---------------------------------------------------------------------"
    echo

    RADICAL_VERBOSE=$OLD_RADICAL_VERBOSE
    RADICAL_PILOT_VERBOSE=$OLD_RADICAL_PILOT_VERBOSE

    profile_event 'verify_rp_install_stop'
}


# ------------------------------------------------------------------------------
# Find available port on the remote host where we can bind to
#
find_available_port()
{
    profile_event 'find_available_port_start'

    RANGE="23000..23100"
    # TODO: Now that we have corrected the logic of checking on the localhost,
    #       instead of the remote host, we need to improve the checking.
    #       For now just return a fixed value.
    AVAILABLE_PORT=23000

    echo ""
    echo "################################################################################"
    echo "## Searching for available TCP port for tunnel in range $RANGE."
    host=$1
    for port in $(eval echo {$RANGE}); do

        # Try to make connection
        (/bin/bash -c "(>/dev/tcp/$host/$port)" 2>/dev/null) &
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

    profile_event 'find_available_port_stop'
}


# -------------------------------------------------------------------------------
#
# run a pre_bootstrap_0 command -- and exit if it happens to fail
#
# pre_bootstrap_0 commands are executed right in arg parser loop because -e can be
# passed multiple times
#
pre_bootstrap_0()
{
    cmd=$(eval echo "$@")

    profile_event 'pre_bootstrap_0_start' "$cmd"

    run_cmd "Running pre_bootstrap_0 command" "$cmd"

    if test $? -ne 0
    then
        echo "#ABORT"
        exit 1
    fi

    profile_event 'pre_bootstrap_0_stop'
}

# -------------------------------------------------------------------------------
#
# Build the PREBOOTSTRAP2 variable to pass down to sub-agents
#
pre_bootstrap_2()
{
    cmd="$@"

    profile_event 'pre_bootstrap_2_start' "$cmd"

    PREBOOTSTRAP2="$PREBOOTSTRAP2
$cmd"

    profile_event 'pre_bootstrap_2_stop'
}


# -------------------------------------------------------------------------------
#
# untar the pilot sandbox
#
untar()
{
    tar="$1"

    profile_event 'untar_start' "$tar"

    tar zxvf ../"$tar" -C .. "$PILOT_ID"

    profile_event 'untar_stop'
}


# ------------------------------------------------------------------------------
#
# MAIN
#

# parse command line arguments
#
# OPTIONS:
#    -a   session sandbox
#    -b   python distribution (default, anaconda)
#    -c   ccm mode of agent startup
#    -d   distribution source tarballs for radical stack install
#    -e   execute commands before bootstrapping phase 1: the main agent
#    -f   tunnel forward endpoint (MongoDB host:port)
#    -h   hostport to create tunnel to
#    -i   python Interpreter to use, e.g., python2.7
#    -j   add a command for the service node
#    -m   mode of stack installion
#    -p   pilot ID
#    -r   radical-pilot version version to install in virtenv
#    -s   session ID
#    -t   tunnel device for connection forwarding
#    -v   virtualenv location (create if missing)
#    -w   execute commands before bootstrapping phase 2: the worker
#    -x   exit cleanup - delete pilot sandbox, virtualenv etc. after completion
#    -y   runtime limit
#    -z   untar initial pilot sandbox tarball
#
# NOTE: -z makes some assumptions on sandbox and tarball location
#
while getopts "a:b:cd:e:f:h:i:m:p:r:s:t:v:w:x:y:z:" OPTION; do
    case $OPTION in
        a)  SESSION_SANDBOX="$OPTARG"         ;;
        b)  PYTHON_DIST="$OPTARG"             ;;
        c)  LAUNCHER='ccmrun'                 ;;
        e)  pre_bootstrap_0 "$OPTARG"         ;;
        f)  FORWARD_TUNNEL_ENDPOINT="$OPTARG" ;;
        h)  HOSTPORT="$OPTARG"                ;;
        i)  PYTHON="$OPTARG"                  ;;
        m)  VIRTENV_MODE="$OPTARG"            ;;
        p)  PILOT_ID="$OPTARG"                ;;
        r)  RP_VERSION="$OPTARG"              ;;
        s)  SESSION_ID="$OPTARG"              ;;
        t)  TUNNEL_BIND_DEVICE="$OPTARG"      ;;
        v)  VIRTENV=$(eval echo "$OPTARG")    ;;
        w)  pre_bootstrap_2 "$OPTARG"         ;;
        x)  CLEANUP="$OPTARG"                 ;;
        y)  RUNTIME="$OPTARG"                 ;;
        z)  TARBALL="$OPTARG"                 ;;
        *)  echo "Unknown option: '$OPTION'='$OPTARG'"
            return 1;;
    esac
done

# WORKAROUND for CONDA setup: not all platforms export corresponding bash
# functions, which could cause an issue during env activation, we ensure
# that these functions are set within `env`. Platform support team is
# responsible to set it correctly (e.g., using `module load`)
for name in $(set | grep -e '^[^ ]*conda[^ ]* ()' | cut -f 1 -d ' ')
do
    export -f $name
done
test "$(set | grep '__add_sys_prefix_to_path ()')" \
 && export -f __add_sys_prefix_to_path

# pre_bootstrap_0 is done at this point, save resulting env
env_dump -t env/bs0_pre_0.env

echo '# -------------------------------------------------------------------'
echo '# untar sandbox'
echo '# -------------------------------------------------------------------'
untar "$TARBALL"
echo '# -------------------------------------------------------------------'

# before we change anything else in the pilot environment, we safe a couple of
# env vars to later re-create a close-to-pristine env for task execution.
_OLD_VIRTUAL_PYTHONPATH="$PYTHONPATH"
_OLD_VIRTUAL_PYTHONHOME="$PYTHONHOME"
_OLD_VIRTUAL_PATH="$PATH"
_OLD_VIRTUAL_PS1="$PS1"

export _OLD_VIRTUAL_PYTHONPATH
export _OLD_VIRTUAL_PYTHONHOME
export _OLD_VIRTUAL_PATH
export _OLD_VIRTUAL_PS1

# derive some var names from given args
if test -z "$SESSION_SANDBOX"
then
    SESSION_SANDBOX="$PILOT_SANDBOX/.."
fi

pilot_state="PMGR_ACTIVE_PENDING"
# FIXME: By now the pre_process rules are already performed.
#        We should split the parsing and the execution of those.
#        "bootstrap start" is here so that $PILOT_ID is known.
# Create header for profile log
echo 'create gtod, prof'
create_gtod
create_prof
profile_event 'bootstrap_0_start'

# NOTE: if the virtenv path contains a symbolic link element, then distutil will
#       report the absolute representation of it, and thus report a different
#       module path than one would expect from the virtenv path.  We thus
#       normalize the virtenv path before we use it.
echo "VIRTENV           : $VIRTENV"
echo "VIRTENV_MODE      : $VIRTENV_MODE"

if test "$VIRTENV_MODE" = 'none'; then
    VIRTENV=''

elif test "$PYTHON_DIST" = "anaconda" && ! test -d "$VIRTENV/"; then
    case "$VIRTENV_MODE" in
        recreate|update|use|local)
            VIRTENV=$(cd `conda info --envs | awk -v envname="$VIRTENV" \
            '{ if ($1 == envname) print $NF }'`; pwd -P)
            ;;
        *)
            echo "WARNING: conda env is set not as directory ($VIRTENV_MODE)"
            ;;
    esac
else
    mkdir -p "$VIRTENV"
    VIRTENV=`(cd $VIRTENV; pwd -P)`
    rmdir "$VIRTENV" 2>/dev/null
fi
echo "VIRTENV normalized: $VIRTENV ($PYTHON_DIST)"

# Check that mandatory arguments are set
# (Currently all that are passed through to the agent)
if test -z "$RUNTIME"     ; then  echo "missing RUNTIME"   ; exit 1;  fi
if test -z "$PILOT_ID"    ; then  echo "missing PILOT_ID"  ; exit 1;  fi
if test -z "$RP_VERSION"  ; then  echo "missing RP_VERSION"; exit 1;  fi

# pilot runtime is specified in minutes -- on shell level, we want seconds
RUNTIME=$((RUNTIME * 60))

# we also add a minute as safety margin, to give the agent proper time to shut
# down on its own
RUNTIME=$((RUNTIME + 60))

# ------------------------------------------------------------------------------
# If the host that will run the agent is not capable of communication
# with the outside world directly, we will setup a tunnel.
get_tunnel(){

    addr=$1

    profile_event 'tunnel_setup_start'

    echo "# -------------------------------------------------------------------"
    echo "# Setting up forward tunnel to $addr."

    # Bind to localhost
    # Check if ifconfig exists
    if test -f /sbin/ifconfig
    then
        BIND_ADDRESS=$(/sbin/ifconfig $TUNNEL_BIND_DEVICE|grep "inet addr"|cut -f2 -d:|cut -f1 -d" ")
        if test -z "$BIND_ADDRESS"
        then
            BIND_ADDRESS=$(/sbin/ifconfig lo0 | grep 'inet' | xargs echo | cut -f 2 -d ' ')
        fi
    else
        BIND_ADDRESS=$(host `hostname` | awk '{print $NF}')
    fi

    if test -z "$BIND_ADDRESS"
    then
        BIND_ADDRESS=$(ip addr             \
                     | grep 'state UP' -A2 \
                     | grep 'inet'         \
                     | awk '{print $2}'    \
                     | cut -f1 -d'/')
      # BIND_ADDRESS="127.0.0.1"
    fi

    # Look for an available port to bind to.
    # This might be necessary if multiple agents run on one host.
    find_available_port $BIND_ADDRESS

    if [ $AVAILABLE_PORT ]; then
        echo "## Found available port: $AVAILABLE_PORT"
    else
        echo "## No available port found!"
        exit 1
    fi
    DBPORT=$AVAILABLE_PORT

    # Set up tunnel
    # TODO: Extract port and host
    FORWARD_TUNNEL_ENDPOINT_PORT=22

    if test -z "$FORWARD_TUNNEL_ENDPOINT"
    then
        FORWARD_TUNNEL_ENDPOINT_HOST=$BIND_ADDRESS

    elif test "$FORWARD_TUNNEL_ENDPOINT" = "BIND_ADDRESS"; then
        # On some systems, e.g. Hopper, sshd on the mom node is not bound to 127.0.0.1
        # In those situations, and if configured, bind to the just obtained bind address.
        FORWARD_TUNNEL_ENDPOINT_HOST=$BIND_ADDRESS

    else
        # FIXME: ensure FT_EP is set
        FORWARD_TUNNEL_ENDPOINT_HOST=$FORWARD_TUNNEL_ENDPOINT
    fi

    # FIXME: check if tunnel stays up
    echo ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:$addr -p $FORWARD_TUNNEL_ENDPOINT_PORT $FORWARD_TUNNEL_ENDPOINT_HOST
         ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:$addr -p $FORWARD_TUNNEL_ENDPOINT_PORT $FORWARD_TUNNEL_ENDPOINT_HOST &

    # Kill ssh process when bootstrap_0 dies, to prevent lingering ssh's
    trap 'jobs -p | grep ssh | xargs -tr -n 1 kill' EXIT

    # and export to agent
    export RP_BS_TUNNEL="$BIND_ADDRESS:$DBPORT"

    profile_event 'tunnel_setup_stop'
}

if ! test -z "$FORWARD_TUNNEL_ENDPOINT"
then
    get_tunnel "$HOSTPORT"
    export RADICAL_PILOT_DB_HOSTPORT="$RP_BS_TUNNEL"
fi

# we also set up a tunnel for the application to use, if a respective endpoint
# is requested in the environment
if ! test -z "$RP_APP_TUNNEL_ADDR"
then
    echo "app tunnel addr : $RP_APP_TUNNEL_ADDR"
    get_tunnel "$RP_APP_TUNNEL_ADDR"
    export RP_APP_TUNNEL="$RP_BS_TUNNEL"
    echo "app tunnel setup: $RP_APP_TUNNEL"
fi

rehash "$PYTHON"

# ready to setup the virtenv
virtenv_setup    "$PILOT_ID"    "$VIRTENV" "$VIRTENV_MODE" \
                 "$PYTHON_DIST"
virtenv_activate "$VIRTENV" "$PYTHON_DIST"
virtenv_eval
create_deactivate

# ------------------------------------------------------------------------------
# launch the radical agent
#

# after all is said and done, we should end up with a usable python version.
# Verify it
verify_install

# all is installed - keep a local copy of `radical-gtod` so that task profiling
# is independent of its location in the pilot VE
test -z $(which radical-gtod) || cp $(which radical-gtod) ./gtod

# TODO: (re)move this output?
echo
echo "# -------------------------------------------------------------------"
echo "# Launching radical-pilot-agent "

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
if test "$?" = 0
then
    RADICAL_PILOT_NTPHOST="46.101.140.169"
fi
echo "ntphost: $RADICAL_PILOT_NTPHOST"
ping -c 1 "$RADICAL_PILOT_NTPHOST" || true  # ignore errors

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
BS_SHELL=$(which bash)
if test -z "$BS_SHELL"
then
    BS_SHELL='/bin/sh'

    if test "$PYTHON_DIST" = "anaconda"
    then
            echo "ERROR: PYTHON_DIST=anaconda but bash not found"
            exit 1
    fi
fi

# disable user site packages as those can conflict with our virtualenv
export PYTHONNOUSERSITE=True

export RP_PILOT_ID="$PILOT_ID"

env_prep -t env/agent.env

# we create a bootstrap_2.sh which sets the environment sub-agents
cat > bootstrap_2.sh <<EOT
#!$BS_SHELL

sid=\$1
reg_addr=\$2
uid=\$3

# activate python environment
. env/agent.env

# pass environment variables down so that module load becomes effective at
# the other side too (e.g. sub-agents).
$PREBOOTSTRAP2_EXPANDED

# start (sub) agent
exec radical-pilot-agent_n "\$sid" "\$reg_addr" "\$uid" "$$" \\
                   1>>"bootstrap_2.\$uid.out" \\
                   2>>"bootstrap_2.\$uid.err"

EOT
chmod 0755 bootstrap_2.sh
# ------------------------------------------------------------------------------

# start the master agent instance (agent_0) in the bs0 environment
profile_event 'bootstrap_0_ok'

$LAUNCHER radical-pilot-agent_0 "$$" 1>>agent_0.out 2>>agent_0.err &

AGENT_PID=$!
pilot_state="PMGR_ACTIVE"

while true
do
    sleep 3
    if kill -0 $AGENT_PID 2>/dev/null
    then
        if test -e "./killme.signal"
        then
            profile_event 'killme' "`date --rfc-3339=ns | cut -c -23`"
            profile_event 'sigterm' "`date --rfc-3339=ns | cut -c -23`"
            echo "send SIGTERM to $AGENT_PID ($$)"
            kill -15 $AGENT_PID
            waitfor 1 30 "kill -0  $AGENT_PID"
            test "$?" = 0 || break

            profile_event 'sigkill' "`date --rfc-3339=ns | cut -c -23`"
            echo "send SIGKILL to $AGENT_PID ($$)"
            kill  -9 $AGENT_PID
        fi
    else
        profile_event 'agent_gone' "`date --rfc-3339=ns | cut -c -23`"
        echo "agent $AGENT_PID is gone"
        break
    fi
done

# collect process and exit code
echo "agent $AGENT_PID is final"
wait $AGENT_PID
AGENT_EXITCODE=$?
echo "agent $AGENT_PID is final ($AGENT_EXITCODE)"
profile_event 'agent_final' "$AGENT_PID:$AGENT_EXITCODE `date --rfc-3339=ns | cut -c -23`"


# # stop the packer.  We don't want to just kill it, as that might leave us with
# # corrupted tarballs...
# touch exit.signal

# cleanup flags:
#   l : pilot log files
#   u : task work dirs
#   v : virtualenv
#   e : everything
echo
echo "# -------------------------------------------------------------------"
echo "# CLEANUP: $CLEANUP"
echo "#"

profile_event 'cleanup_start'
contains $CLEANUP 'l' && rm -r "$PILOT_SANDBOX/agent_*"
contains $CLEANUP 'u' && rm -r "$PILOT_SANDBOX/task.*"
contains $CLEANUP 'v' && rm -r "$VIRTENV/" # FIXME: in what cases?
contains $CLEANUP 'e' && rm -r "$PILOT_SANDBOX/"
profile_event 'cleanup_stop'

echo "#"
echo "# -------------------------------------------------------------------"

if ! test -z "`ls *.prof 2>/dev/null`"
then
    echo
    echo "# -------------------------------------------------------------------"
    echo "#"
    echo "# Mark final profiling entry ..."
    last_event
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

if ! test -z "`ls *{log,out,err,cfg} 2>/dev/null`"
then
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
echo "#"
echo "# Done, exiting ($AGENT_EXITCODE)"
echo "#"
echo "# -------------------------------------------------------------------"

# ... and exit
exit $AGENT_EXITCODE

