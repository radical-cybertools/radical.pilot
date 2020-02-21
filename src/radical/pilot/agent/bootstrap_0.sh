#!/bin/bash -l

# Unset functions/aliases of commands that will be used during bootsrap as
# these custom functions can break assumed/expected behavior
export PS1='#'
unset PROMPT_COMMAND
unset -f cd ls uname pwd date bc cat echo

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

# ------------------------------------------------------------------------------
# Copyright 2013-2015, RADICAL @ Rutgers
# Licensed under the MIT License
#
# This script launches a radical.pilot compute pilot.  If needed, it creates and
# populates a virtualenv on the fly, into $VIRTENV.
#
# https://xkcd.com/1987/
#
# A created virtualenv will contain all dependencies for the RADICAL stack (see
# $VIRTENV_RADICAL_DEPS).  The RADICAL stack itself (or at least parts of it,
# see $VIRTENV_RADICAL_MODS) will be installed into $VIRTENV/radical/, and
# PYTHONPATH will be set to include that tree during runtime.  That allows us to
# use a different RADICAL stack if needed, by rerouting the PYTHONPATH, w/o the
# need to create a new virtualenv from scratch.
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
SDISTS=
RUNTIME=
VIRTENV=
VIRTENV_MODE=
CCM=
PILOT_ID=
RP_VERSION=
PYTHON=
PYTHON_DIST=
VIRTENV_DIST=
SESSION_ID=
SESSION_SANDBOX=
PILOT_SANDBOX=`pwd`
PREBOOTSTRAP2=""

# NOTE:  $HOME is set to the job sandbox on OSG.  Bah!
# FIXME: the need for this needs to be reconfirmed and documented
# mkdir -p .ssh/

# flag which is set when a system level RP installation is found, triggers
# '--upgrade' flag for pip
# NOTE: this mechanism is disabled, as it affects a minority of machines and
#       adds too much complexity for too little benefit.  Also, it will break on
#       machines where pip has no connectivity, and pip cannot silently ignore
#       that system version...
# SYSTEM_RP='FALSE'


# seconds to wait for lock files
# 10 min should be enough for anybody to create/update a virtenv...
LOCK_TIMEOUT=600 # 10 min

VIRTENV_VER="virtualenv-16.7.5"
VIRTENV_DIR="$VIRTENV_VER"
VIRTENV_TGZ="$VIRTENV_VER.tar.gz"
VIRTENV_TGZ_URL="https://files.pythonhosted.org/packages/66/f0/6867af06d2e2f511e4e1d7094ff663acdebc4f15d4a0cb0fed1007395124/$VIRTENV_TGZ"
VIRTENV_IS_ACTIVATED=FALSE

VIRTENV_RADICAL_DEPS="pymongo colorama python-hostlist ntplib "\
"pyzmq netifaces setproctitle msgpack future regex munch"

VIRTENV_RADICAL_MODS="pymongo colorama hostlist ntplib "\
"zmq netifaces setproctitle msgpack future regex"

if ! test -z "$RADICAL_DEBUG"
then
    VIRTENV_RADICAL_DEPS="$VIRTENV_RADICAL_DEPS pudb"
fi


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
# If profiling is enabled, compile our little gtod app and take the first time
#
create_gtod()
{
    tmp=`date '+%s.%N' | grep N`
    if test "$?" = 0
    then
        if ! contains "$tmp" '%'
        then
            # we can use the system tool
            echo "#!/bin/sh"       > ./gtod
            echo "date '+%s.%6N'" >> ./gtod
        fi
    else
        shell=/bin/sh
        test -x '/bin/bash' && shell=/bin/bash

        echo "#!$SHELL"                                                > ./gtod
        echo "if test -z \"\$EPOCHREALTIME\""                         >> ./gtod
        echo "then"                                                   >> ./gtod
        echo "  python3 -c 'import time;print(\"%.6f\"%time.time())'" >> ./gtod
        echo "else"                                                   >> ./gtod
        echo "  echo \${EPOCHREALTIME:0:20}"                          >> ./gtod
        echo "fi"                                                     >> ./gtod
    fi

    chmod 0755 ./gtod

    set -x
    TIME_ZERO=`./gtod`
    export TIME_ZERO
    set +x
}


# ------------------------------------------------------------------------------
#
profile_event()
{
    set -x
    if test -z "$RADICAL_PILOT_PROFILE$RADICAL_PROFILE"
    then
        return
    fi

    PROFILE="bootstrap_0.prof"

    event=$1
    msg=$2

    epoch=`./gtod`
    now=$(awk "BEGIN{print($epoch - $TIME_ZERO)}")

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
        "$now" "$event" "bootstrap_0" "MainThread" "$PILOT_ID" "PMGR_ACTIVE_PENDING" "$msg" \
        | tee -a "$PROFILE"
    set +x
}


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
    INTERVAL="$1"; shift
    TIMEOUT="$1";  shift
    COMMAND="$*"

    START=`echo \`./gtod\` | cut -f 1 -d .`
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
        NOW=`echo \`./gtod\` | cut -f 1 -d .`
    done

    if test "$RET" = 0
    then
        echo "COND timeout"
    fi

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
            echo "can't create lockfile at '$lockfile' - invalid directory?"
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
        err=`/bin/bash -c "set -C ; echo $pid > '$lockfile' && chmod a+r '$lockfile' && echo ok" 2>&1`
    done

    # one way or the other, we got the lock finally.
    echo "obtained lock $lockfile"
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

    rm -vf $lockfile
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
    explicit_python="$1"

    # If PYTHON was not set as an argument, detect it here.
    # we need to do this again after the virtenv is loaded
    if test -z "$explicit_python"
    then
        PYTHON=`which python`
    else
        PYTHON="$explicit_python"
    fi

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

    if test -f "$CA_CERT_PEM"
    then
        PIP="`which pip` --cert $CA_CERT_PEM"
    else
        PIP="`which pip`"
    fi

    # NOTE: some resources define a function pip() to implement the same cacert
    #       fix we do above.  On some machines, that is broken (hello archer),
    #       thus we undefine that function here.
    unset -f pip

    echo "PYTHON: $PYTHON"
    echo "PIP   : $PIP"
}


# ------------------------------------------------------------------------------
# verify that we have a usable python installation
verify_install()
{
    echo -n "verify python viability: $PYTHON ..."
    if ! $PYTHON -c 'import sys; assert(sys.version_info >= (3,5))'
    then
        echo ' failed'
        echo "python installation ($PYTHON) is not usable - abort"
        exit 1
    fi
    echo ' ok'

    # FIXME: attempt to load all required modules
    modules="radical.pilot radical.saga radical.utils $VIRTENV_RADICAL_MODS"
    for m in $modules
    do
        printf 'verify module viability: %-15s ...' $m
        if ! $PYTHON -c "import $m"
        then
            echo ' failed'
            echo "python installation cannot load module $m - abort"
            exit 1
        fi
        echo ' ok'

    done
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
# bootstrap_0 invokations.
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
    profile_event 've_setup_start'

    pid="$1"
    virtenv="$2"
    virtenv_mode="$3"
    python_dist="$4"
    virtenv_dist="$5"

    ve_create=UNDEFINED
    ve_update=UNDEFINED

    if test "$virtenv_mode" = "private"
    then
        if test -d "$virtenv/"
        then
            printf "\nERROR: private virtenv already exists at $virtenv\n\n"
            exit 1
        fi
        ve_create=TRUE
        ve_update=FALSE

    elif test "$virtenv_mode" = "update"
    then
        ve_create=FALSE
        ve_update=TRUE
        test -d "$virtenv/" || ve_create=TRUE
    elif test "$virtenv_mode" = "create"
    then
        ve_create=TRUE
        ve_update=FALSE

    elif test "$virtenv_mode" = "use"
    then
        if ! test -d "$virtenv/"
        then
            printf "\nERROR: given virtenv does not exist at $virtenv\n\n"
            exit 1
        fi
        ve_create=FALSE
        ve_update=FALSE

    elif test "$virtenv_mode" = "recreate"
    then
        test -d "$virtenv/" && rm -r "$virtenv"
        ve_create=TRUE
        ve_update=FALSE
    else
        ve_create=FALSE
        ve_update=FALSE
        printf "\nERROR: virtenv mode invalid: $virtenv_mode\n\n"
        exit 1
    fi

    if test "$ve_create" = 'TRUE'
    then
        # no need to update a fresh ve
        ve_update=FALSE
    fi

    echo "virtenv_create   : $ve_create"
    echo "virtenv_update   : $ve_update"


    # radical_pilot installation and update is governed by PILOT_VERSION.  If
    # that is set to 'stage', we install the release and use the pilot which was
    # staged to pwd.  If set to 'release', we install from pypi.  In all other
    # cases, we install from git at a specific tag or branch
    #
    case "$RP_VERSION" in

        local)
            for sdist in `echo $SDISTS | tr ':' ' '`
            do
                src=${sdist%.tgz}
                src=${sdist%.tar.gz}
                # NOTE: Condor does not support staging into some arbitrary
                #       directory, so we may find the dists in pwd
                if test -e   "$SESSION_SANDBOX/$sdist"; then
                    tar zxmf "$SESSION_SANDBOX/$sdist"

                elif test -e "./$sdist"; then
                    tar zxmf "./$sdist"

                else
                    echo "missing $sdist"
                    echo "session sandbox: $SESSION_SANDBOX"
                    ls -la "$SESSION_SANDBOX"
                    echo "pilot sandbox: $(pwd)"
                    ls -la
                    exit 1
                fi
                RP_INSTALL_SOURCES="$RP_INSTALL_SOURCES $src/"
            done
            RP_INSTALL_TARGET='SANDBOX'
            RP_INSTALL_SDIST='TRUE'
            ;;

        release)
            RP_INSTALL_SOURCES='radical.pilot'
            RP_INSTALL_TARGET='SANDBOX'
            RP_INSTALL_SDIST='FALSE'
            ;;

        installed)
            RP_INSTALL_SOURCES=''
            RP_INSTALL_TARGET=''
            RP_INSTALL_SDIST='FALSE'
            ;;

        *)
            # NOTE: do *not* use 'pip -e' -- egg linking does not work with
            #       PYTHONPATH.  Instead, we manually clone the respective
            #       git repository, and switch to the branch/tag/commit.
            git clone https://github.com/radical-cybertools/radical.pilot.git
            (cd radical.pilot; git checkout $RP_VERSION)
            RP_INSTALL_SOURCES="radical.pilot/"
            RP_INSTALL_TARGET='SANDBOX'
            RP_INSTALL_SDIST='FALSE'
    esac

    # NOTE: for any immutable virtenv (VIRTENV_MODE==use), we have to choose
    #       a SANDBOX install target.  SANDBOX installation will only work with
    #       'python setup.py install' (pip cannot handle it), so we have to use
    #       the sdist, and the RP_INSTALL_SOURCES has to point to directories.
    if test "$virtenv_mode" = "use"
    then
        if test "$RP_INSTALL_TARGET" = "VIRTENV"
        then
            echo "WARNING: virtenv immutable - install RP locally"
            RP_INSTALL_TARGET='SANDBOX'
        fi

        if ! test -z "$RP_INSTALL_TARGET"
        then
            for src in $RP_INSTALL_SOURCES
            do
                if ! test -d "$src"
                then
                    # TODO: we could in principle download from pypi and
                    # extract, or 'git clone' to local, and then use the setup
                    # install.  Not sure if this is worth the effor (AM)
                    echo "ERROR: local RP install needs sdist based install (not '$src')"
                    exit 1
                fi
            done
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
            echo 'rp lock for ve create'
            lock "$pid" "$virtenv" # use default timeout
            virtenv_create "$virtenv" "$python_dist" "$virtenv_dist"
            if ! test "$?" = 0
            then
               echo "Error on virtenv creation -- abort"
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
        echo 'rp lock for ve update'
        lock "$pid" "$virtenv" # use default timeout
        virtenv_update "$virtenv" "$python_dist"
        if ! test "$?" = 0
        then
           echo "Error on virtenv update -- abort"
           unlock "$pid" "$virtenv"
           exit 1
       fi
       unlock "$pid" "$virtenv"
    else
        echo "do not update virtenv $virtenv"
    fi

    # install RP
    if test "$RP_INSTALL_LOCK" = 'TRUE'
    then
        echo "rp lock for rp install (target: $RP_INSTALL_TARGET)"
        lock "$pid" "$virtenv" # use default timeout
    fi
    rp_install "$RP_INSTALL_SOURCES" "$RP_INSTALL_TARGET" "$RP_INSTALL_SDIST"
    if test "$RP_INSTALL_LOCK" = 'TRUE'
    then
       unlock "$pid" "$virtenv"
    fi

    profile_event 've_setup_stop'
}


# ------------------------------------------------------------------------------
#
virtenv_activate()
{
    profile_event 've_activate_start'

    virtenv="$1"
    python_dist="$2"

    if test "$VIRTENV_IS_ACTIVATED" = "TRUE"
    then
        return
    fi

    if test "$python_dist" = "anaconda"
    then
        source activate $virtenv/
    else
        unset VIRTUAL_ENV
        . "$virtenv/bin/activate"
        if test -z "$VIRTUAL_ENV"
        then
            echo "Loading of virtual env failed!"
            exit 1
        fi

    fi
    VIRTENV_IS_ACTIVATED=TRUE

    # make sure we use the new python binary
    rehash

  # # NOTE: calling radicalpilot-version does not work here -- depending on the
  # #       system settings, python setup it may not be found even if the
  # #       rp module is installed and importable.
  # system_rp_loc="`python -c 'import radical.pilot as rp; print rp.__file__' 2>/dev/null`"
  # if ! test -z "$system_rp_loc"
  # then
  #     echo "found system RP install at '$system_rp_loc'"
  #     SYSTEM_RP='TRUE'
  # fi

    prefix="$virtenv/rp_install"

    # make sure the lib path into the prefix conforms to the python conventions
    PYTHON_VERSION=`$PYTHON -c 'import distutils.sysconfig as sc; print(sc.get_python_version())'`
    VE_MOD_PREFIX=` $PYTHON -c 'import distutils.sysconfig as sc; print(sc.get_python_lib())'`
    echo "PYTHON INTERPRETER: $PYTHON"
    echo "PYTHON_VERSION    : $PYTHON_VERSION"
    echo "VE_MOD_PREFIX     : $VE_MOD_PREFIX"
    echo "PIP installer     : $PIP"
    echo "PIP version       : `$PIP --version`"

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

    # we can now derive the pythonpath into the rp_install portion by replacing
    # the leading path elements.  The same mechanism is used later on
    # to derive the PYTHONPATH into the sandbox rp_install, if needed.
    RP_MOD_PREFIX=`echo $VE_MOD_PREFIX | sed -e "s|$virtenv|$virtenv/rp_install|"`
    VE_PYTHONPATH="$PYTHONPATH"
    RP_PATH="$virtenv/bin"

    # NOTE: this should not be necessary, but we explicit set PYTHONPATH to
    #       include the VE module tree, because some systems set a PYTHONPATH on
    #       'module load python', and that would supersede the VE module tree,
    #       leading to unusable versions of setuptools.
    PYTHONPATH="$VE_MOD_PREFIX:$VE_PYTHONPATH"
    export PYTHONPATH

    PATH="$RP_PATH:$PATH"
    export $PATH

    echo "activated virtenv"
    echo "VIRTENV      : $virtenv"
    echo "VE_PYTHONPATH: $VE_PYTHONPATH"
    echo "VE_MOD_PREFIX: $VE_MOD_PREFIX"
    echo "RP_MOD_PREFIX: $RP_MOD_PREFIX"
    echo "RP_PATH      : $RP_PATH"

    profile_event 've_activate_stop'
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
    profile_event 've_create_start'

    virtenv="$1"
    python_dist="$2"
    virtenv_dist="$3"

    if test "$python_dist" = "default"
    then

        # by default, we download an older 1.9.x version of virtualenv as this
        # seems to work more reliable than newer versions, on some machines.
        # Only on machines where the system virtenv seems to be more stable or
        # where 1.9 is known to fail, we use the system ve.
        if test "$virtenv_dist" = "default"
        then
            virtenv_dist="1.9"
        fi

        if test "$virtenv_dist" = "1.9"
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
                echo "WARNING: Couldn't download virtualenv via curl! Using system version."
                virtenv_dist="system"

            else :
                run_cmd "unpacking virtualenv tgz" \
                        "tar zxmf '$VIRTENV_TGZ'"

                if test $? -ne 0
                then
                    echo "Couldn't unpack virtualenv! Using systemv version"
                    virtenv_dist="default"
                else
                    VIRTENV_CMD="$PYTHON $VIRTENV_DIR/virtualenv.py"
                fi

            fi
        fi

        # don't use `elif` here - above falls back to 'system' virtenv on errors
        if test "$virtenv_dist" = "system"
        then
            VIRTENV_CMD="virtualenv"
        fi

        if test "$VIRTENV_CMD" = ""
        then
            echo "ERROR: invalid or unusable virtenv_dist option"
            return 1
        fi

        run_cmd "Create virtualenv" \
                "$VIRTENV_CMD -p python3 $virtenv"

        if test $? -ne 0
        then
            echo "ERROR: Couldn't create virtualenv"
            return 1
        fi

        # clean out virtenv sources
        if test -d "$VIRTENV_DIR"
        then
            rm -rf "$VIRTENV_DIR" "$VIRTENV_TGZ"
        fi


    elif test "$python_dist" = "anaconda"
    then
        run_cmd "Create virtualenv" \
                "conda create -y -p $virtenv python=3.8"
        if test $? -ne 0
        then
            echo "ERROR: Couldn't create virtualenv"
            return 1
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
    PIP="$(which pip)"

    # make sure the new pip version is used (but keep the python executable)
    rehash "$PYTHON"

    # now that the virtenv is set up, we install all dependencies
    # of the RADICAL stack
    for dep in $VIRTENV_RADICAL_DEPS
    do
        run_cmd "install $dep" \
                "$PIP --no-cache-dir install --no-build-isolation $dep" \
             || echo "Couldn't install $dep! Lets see how far we get ..."
    done

    profile_event 've_create_stop'
}


# ------------------------------------------------------------------------------
#
# update virtualenv - this assumes that the virtenv has been activated
#
virtenv_update()
{
    profile_event 've_update_start'

    virtenv="$1"
    pytohn_dist="$2"
    virtenv_activate "$virtenv" "$python_dist"

    # we upgrade all dependencies of the RADICAL stack, one by one.
    # NOTE: we only do pip upgrades -- that will ignore the easy_installed
    #       modules on india etc.
    for dep in $VIRTENV_RADICAL_DEPS
    do
        run_cmd "install $dep" \
                "$PIP install --upgrade $dep" \
             || echo "Couldn't update $dep! Lets see how far we get ..."
    done

    profile_event 've_update_stop'
}


# ------------------------------------------------------------------------------
#
# Install the radical stack, ie. install RP which pulls the rest.
# This assumes that the virtenv has been activated.  Any previously installed
# stack version is deleted.
#
# As the virtenv should have all dependencies set up (see VIRTENV_RADICAL_DEPS),
# we don't expect any additional module pull from pypi.  Some rp_versions will,
# however, pull the rp modules from pypi or git.
#
# . $VIRTENV/bin/activate
# rm -rf $VIRTENV/rp_install
#
# case rp_version:
#   @<token>:
#   @tag/@branch/@commit: # no sdist staging
#       git clone $github_base radical.pilot.src
#       (cd radical.pilot.src && git checkout token)
#       pip install -t $SANDBOX/rp_install/ radical.pilot.src
#       rm -rf radical.pilot.src
#       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
#       export PATH=$SANDBOX/rp_install/bin:$PATH
#
#   release: # no sdist staging
#       pip install -t $SANDBOX/rp_install radical.pilot
#       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
#       export PATH=$SANDBOX/rp_install/bin:$PATH
#
#   local: # needs sdist staging
#       tar zxmf $sdist.tgz
#       pip install -t $SANDBOX/rp_install $sdist/
#       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
#       export PATH=$SANDBOX/rp_install/bin:$PATH
#
#   installed: # no sdist staging
#       true
# esac
#
# NOTE: A 'pip install' (without '--upgrade') will not install anything if an
#       old version lives in the system space.  A 'pip install --upgrade' will
#       fail if there is no network connectivity (which otherwise is not really
#       needed when we install from sdists).  '--upgrade' is not needed when
#       installing from sdists.
#
rp_install()
{
    rp_install_sources="$1"
    rp_install_target="$2"
    rp_install_sdist="$3"

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

    profile_event 'rp_install_start'

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

            # NOTE: we first uninstall RP (for some reason, 'pip install --upgrade' does
            #       not work with all source types)
            run_cmd "uninstall radical.pilot" "$PIP uninstall -y radical.pilot"
            # ignore any errors

            echo "using virtenv install tree"
            echo "PYTHONPATH: $PYTHONPATH"
            echo "rp_install: $RP_MOD_PREFIX"
            echo "radicalmod: $RADICAL_MOD_PREFIX"
            ;;

        SANDBOX)
            RP_INSTALL="$PILOT_SANDBOX/rp_install"

            # make sure the lib path into the prefix conforms to the python conventions
            RP_LOC_PREFIX=`echo $VE_MOD_PREFIX | sed -e "s|$VIRTENV|$PILOT_SANDBOX/rp_install|"`

            echo "VE_MOD_PREFIX: $VE_MOD_PREFIX"
            echo "VIRTENV      : $VIRTENV"
            echo "SANDBOX      : $PILOT_SANDBOX"
            echo "VE_LOC_PREFIX: $VE_LOC_PREFIX"

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
            echo "PYTHONPATH: $PYTHONPATH"
            echo "rp_install: $RP_LOC_PREFIX"
            echo "radicalmod: $RADICAL_MOD_PREFIX"
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

  # # NOTE: if we find a system level RP install, then pip install will not work
  # #       w/o the upgrade flag -- unless we install from sdist.  It may not
  # #       work with update flag either though...
  # if test "$SYSTEM_RP" = 'FALSE'
  # then
  #     # no previous version installed, don't need no upgrade
  #     pip_flags=''
  #     echo "no previous RP version - no upgrade"
  # else
  #     if test "$rp_install_sdist" = "TRUE"
  #     then
  #         # install from sdist doesn't need uprade either
  #         pip_flags=''
  #     else
  #         pip_flags='--upgrade'
  #         # NOTE: --upgrade is unreliable in its results -- depending on the
  #         #       VE setup, the resulting installation may be viable or not.
  #         echo "-----------------------------------------------------------------"
  #         echo " WARNING: found a system installation of radical.pilot!          "
  #         echo "          Upgrading to a new version may *or may not* succeed,   "
  #         echo "          depending on the specific system, python and virtenv   "
  #         echo "          configuration!                                         "
  #         echo "-----------------------------------------------------------------"
  #     fi
  # fi

    pip_flags="$pip_flags --src '$PILOT_SANDBOX/rp_install/src'"
    pip_flags="$pip_flags --build '$PILOT_SANDBOX/rp_install/build'"
    pip_flags="$pip_flags --install-option='--prefix=$RP_INSTALL'"
    pip_flags="$pip_flags --no-deps --no-cache-dir --no-build-isolation"

    for src in $rp_install_sources
    do
        run_cmd "update $src via pip" \
                "$PIP install $pip_flags $src"

        if test $? -ne 0
        then
            echo "Couldn't install $src! Lets see how far we get ..."
        fi

        # NOTE: why? fuck pip, that's why!
        rm -rf "$PILOT_SANDBOX/rp_install/build"

        # clean out the install source if it is a local dir
        if test -d "$src"
        then
            echo "purge install source at $src"
            rm -r "$src"
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
    radical-stack
 (  $PYTHON -c 'import radical.utils as ru; print("RU: %s %s" % (ru.version_detail, ru.__file__))' \
 && $PYTHON -c 'import radical.saga  as rs; print("RS: %s %s" % (rs.version_detail, rs.__file__))' \
 && $PYTHON -c 'import radical.pilot as rp; print("RP: %s %s" % (rp.version_detail, rp.__file__))' \
 && (echo 'install ok!'; true) \
 ) \
 || (echo 'install failed!'; false) \
 || exit 1
    echo
    echo "---------------------------------------------------------------------"
    echo

    RADICAL_VERBOSE=$OLD_RADICAL_VERBOSE
    RADICAL_PILOT_VERBOSE=$OLD_RADICAL_PILOT_VERBOSE
}


# ------------------------------------------------------------------------------
# Find available port on the remote host where we can bind to
#
find_available_port()
{
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
pre_bootstrap_2()
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
echo "bootstrap_0 started as     : '$0 $@'"
echo "Environment of bootstrap_0 process:"

# print the sorted env for logging, but also keep a copy so that we can dig
# original env settings for any CUs, if so specified in the resource config.
env | sort | grep '=' | tee env.orig
echo "# -------------------------------------------------------------------"

# parse command line arguments
#
# OPTIONS:
#    -a   session sandbox
#    -b   python distribution (default, anaconda)
#    -c   ccm mode of agent startup
#    -d   distribution source tarballs for radical stack install
#    -e   execute commands before bootstrapping phase 1: the main agent
#    -f   tunnel forward endpoint (MongoDB host:port)
#    -g   virtualenv distribution (default, 1.9, system)
#    -h   hostport to create tunnel to
#    -i   python Interpreter to use, e.g., python2.7
#    -m   mode of stack installion
#    -p   pilot ID
#    -r   radical-pilot version version to install in virtenv
#    -s   session ID
#    -t   tunnel device for connection forwarding
#    -v   virtualenv location (create if missing)
#    -w   execute commands before bootstrapping phase 2: the worker
#    -x   exit cleanup - delete pilot sandbox, virtualenv etc. after completion
#    -y   runtime limit
#
while getopts "a:b:cd:e:f:g:h:i:m:p:r:s:t:v:w:x:y:" OPTION; do
    case $OPTION in
        a)  SESSION_SANDBOX="$OPTARG"  ;;
        b)  PYTHON_DIST="$OPTARG"  ;;
        c)  CCM='TRUE'  ;;
        d)  SDISTS="$OPTARG"  ;;
        e)  pre_bootstrap_0 "$OPTARG"  ;;
        f)  FORWARD_TUNNEL_ENDPOINT="$OPTARG"  ;;
        g)  VIRTENV_DIST="$OPTARG"  ;;
        h)  HOSTPORT="$OPTARG"  ;;
        i)  PYTHON="$OPTARG"  ;;
        m)  VIRTENV_MODE="$OPTARG"  ;;
        p)  PILOT_ID="$OPTARG"  ;;
        r)  RP_VERSION="$OPTARG"  ;;
        s)  SESSION_ID="$OPTARG"  ;;
        t)  TUNNEL_BIND_DEVICE="$OPTARG" ;;
        v)  VIRTENV=$(eval echo "$OPTARG")  ;;
        w)  pre_bootstrap_2 "$OPTARG"  ;;
        x)  CLEANUP="$OPTARG"  ;;
        y)  RUNTIME="$OPTARG"  ;;
        *)  echo "Unknown option: '$OPTION'='$OPTARG'"
            return 1;;
    esac
done

# before we change anything else in the pilot environment, we safe a couple of
# env vars to later re-create a close-to-pristine env for unit execution.
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

# TODO: Move earlier, because if pre_bootstrap fails, this is not yet set
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

# FIXME: By now the pre_process rules are already performed.
#        We should split the parsing and the execution of those.
#        "bootstrap start" is here so that $PILOT_ID is known.
# Create header for profile log
if ! test -z "$RADICAL_PILOT_PROFILE$RADICAL_PROFILE"
then
    echo 'create gtod'
    create_gtod
else
    echo 'create gtod'
    create_gtod
fi
profile_event 'bootstrap_0_start'

# NOTE: if the virtenv path contains a symbolic link element, then distutil will
#       report the absolute representation of it, and thus report a different
#       module path than one would expect from the virtenv path.  We thus
#       normalize the virtenv path before we use it.
mkdir -p "$VIRTENV"
echo "VIRTENV : $VIRTENV"
VIRTENV=`(cd $VIRTENV; pwd -P)`
echo "VIRTENV : $VIRTENV (normalized)"
rmdir "$VIRTENV" 2>/dev/null

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
        # FIXME: ensur FT_EP is set
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
                 "$PYTHON_DIST" "$VIRTENV_DIST"
virtenv_activate "$VIRTENV" "$PYTHON_DIST"

# ------------------------------------------------------------------------------
# launch the radical agent
#
# the actual agent script lives in PWD if it was staged -- otherwise we use it
# from the virtenv
# NOTE: For some reasons, I have seen installations where 'scripts' go into
#       bin/, and some where setuptools only changes them in place.  For now,
#       we allow for both -- but eventually (once the agent itself is small),
#       we may want to move it to bin ourself.  At that point, we probably
#       have re-implemented pip... :/
# FIXME: the second option should use $RP_MOD_PATH, or should derive the path
#       from the imported rp modules __file__.
PILOT_SCRIPT=`which radical-pilot-agent`
# if test "$RP_INSTALL_TARGET" = 'PILOT_SANDBOX'
# then
#     PILOT_SCRIPT="$PILOT_SANDBOX/rp_install/bin/radical-pilot-agent"
# else
#     PILOT_SCRIPT="$VIRTENV/rp_install/bin/radical-pilot-agent"
# fi

# after all is said and done, we should end up with a usable python version.
# Verify it
verify_install

AGENT_CMD="$PYTHON $PILOT_SCRIPT"

verify_rp_install

# TODO: (re)move this output?
echo
echo "# -------------------------------------------------------------------"
echo "# Launching radical-pilot-agent "
echo "# CMDLINE: $AGENT_CMD"

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
if test "$PYTHON_DIST" = "anaconda"
then
    BS_SHELL='/bin/bash'
else
    BS_SHELL='/bin/sh'
fi

cat > bootstrap_2.sh <<EOT
#!$BS_SHELL

# some inspection for logging
hostname

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
export PATH=$PATH

# run agent in debug mode
# FIXME: make option again?
export RADICAL_VERBOSE=DEBUG
export RADICAL_UTIL_VERBOSE=DEBUG
export RADICAL_PILOT_VERBOSE=DEBUG

# the agent will *always* use the dburl from the config file, not from the env
# FIXME: can we better define preference in the session ctor?
unset RADICAL_PILOT_DBURL

# avoid ntphost lookups on compute nodes
export RADICAL_PILOT_NTPHOST=$RADICAL_PILOT_NTPHOST

# pass environment variables down so that module load becomes effective at
# the other side too (e.g. sub-agents).
$PREBOOTSTRAP2_EXPANDED

# start agent, forward arguments
# NOTE: exec only makes sense in the last line of the script
exec $AGENT_CMD "\$1" 1>"\$1.out" 2>"\$1.err"

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
profile_event 'sync_rel' 'agent.0'


# # I am ashamed that we have to resort to this -- lets hope it's temporary...
# cat > packer.sh <<EOT
# #!/bin/sh
#
# PROFILES_TARBALL="$PILOT_ID.prof.tgz"
# LOGFILES_TARBALL="$PILOT_ID.log.tgz"
#
# echo "start packing profiles / logfiles [\$(date)]"
# while ! test -e exit.signal
# do
#
#     if test -z "\$(ls *.prof )"
#     then
#         echo "skip  packing profiles [\$(date)]"
#     else
#         echo "check packing profiles [\$(date)]"
#         mkdir prof/
#         cp  *.prof prof/
#         tar -czf "\$PROFILES_TARBALL.tmp" prof/ || true
#         mv       "\$PROFILES_TARBALL.tmp" "\$PROFILES_TARBALL"
#         rm -rf prof/
#     fi
#
#
#     # we always have a least the cfg file
#     if true
#     then
#         echo "check packing logfiles [\$(date)]"
#         mkdir log/
#         cp  *.log *.out *.err *,cfg log/
#         tar -czf "\$LOGFILES_TARBALL.tmp" log/ || true
#         mv       "\$LOGFILES_TARBALL.tmp" "\$LOGFILES_TARBALL"
#         rm -rf log/
#     fi
#
#     ls -l *.tgz
#     sleep 10
# done
# echo "stop  packing profiles / logfiles [\$(date)]"
# EOT
# chmod 0755 packer.sh
# ./packer.sh 2>&1 >> bootstrap_0.out &
# PACKER_ID=$!

if test -z "$CCM"; then
    ./bootstrap_2.sh 'agent.0'    \
                   1> agent.0.bootstrap_2.out \
                   2> agent.0.bootstrap_2.err &
else
    ccmrun ./bootstrap_2.sh 'agent.0'    \
                   1> agent.0.bootstrap_2.out \
                   2> agent.0.bootstrap_2.err &
fi
AGENT_PID=$!

while true
do
    sleep 3
    if kill -0 $AGENT_PID 2>/dev/null
    then
        echo -n '.'
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
        echo
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
#   u : unit work dirs
#   v : virtualenv
#   e : everything
echo
echo "# -------------------------------------------------------------------"
echo "# CLEANUP: $CLEANUP"
echo "#"

profile_event 'cleanup_start'
contains $CLEANUP 'l' && rm -r "$PILOT_SANDBOX/agent.*"
contains $CLEANUP 'u' && rm -r "$PILOT_SANDBOX/unit.*"
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
echo "# push final pilot state: $SESSION_ID $PILOT_ID $final_state"
sp=$(which radical-pilot-agent-statepush)
test -z "$sp" && echo "statepush not found"
test -z "$sp" || $PYTHON "$sp" agent.0.cfg "$final_state"

echo
echo "# -------------------------------------------------------------------"
echo "#"
echo "# Done, exiting ($AGENT_EXITCODE)"
echo "#"
echo "# -------------------------------------------------------------------"

# ... and exit
exit $AGENT_EXITCODE

