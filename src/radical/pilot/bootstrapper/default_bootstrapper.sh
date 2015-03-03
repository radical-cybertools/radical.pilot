#!/bin/bash -l

# ------------------------------------------------------------------------------
# Copyright 2013-2014, radical@rutgers.edu License under the MIT License
#
# This script launches a radical.pilot compute pilot.  If needed, it creates and
# populates a virtualenv on the fly, into $VIRTENV.
#
# A created virtualenv will contain all dependencies for the RADICAL stack (see
# $VIRTENV_RADICAL_DEPS).  The RADICAL stack itself (or at least parts of it,
# see $VIRTENV_RADICAL_MODS) will be installed into $VIRTENV/radical/, and
# PYTHONPATH will be set to include that tree during runtime.  That allows us to
# use a different RADICAL stack if needed, by rerouting the PYTHONPATH, w/o the
# need to create a new virtualenv from scratch.
#
# ------------------------------------------------------------------------------
# global variables
#
AUTH=
TUNNEL_BIND_DEVICE="lo"
CLEANUP=
CORES=
DBNAME=
DBURL=
DEBUG=
SDISTS=
VIRTENV=
VIRTENV_MODE=
LRMS=
MPI_LAUNCH_METHOD=
SPAWNER=
PILOT_ID=
RP_VERSION=
AGENT_TYPE=
PYTHON=
RUNTIME=
SCHEDULER=
SESSIONID=
TASK_LAUNCH_METHOD=
SANDBOX=`pwd`


# seconds to wait for lock files
# 10 min should be enough for anybody to create/update a virtenv...
LOCK_TIMEOUT=600
VIRTENV_TGZ_URL="https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz"
VIRTENV_TGZ="virtualenv-1.9.tar.gz"
VIRTENV_IS_ACTIVATED=FALSE
VIRTENV_RADICAL_DEPS="pymongo apache-libcloud colorama python-hostlist"


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
        printf '  %12s : %-20s : %12.4f : %-17s : %-24s : %-40s : \n' \
                  ' '    ' '     "$NOW"   ' '     'Bootstrap' "$@"    \
        >> agent.prof
    fi
}

profile_event 'bootstrap start'


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

    err=`/bin/bash -c "set -C ; echo $pid > '$lockfile' && echo ok" 2>&1`
    until ! test "$err" = "ok"
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
        err=`/bin/bash -c "set -C ; echo $pid > '$lockfile' && echo ok" 2>&1`
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
    eval $cmd
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
            eval $fallback
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
        printf "\n\tERROR: $msg\n\n" >> /dev/stderr
    fi

    cat << EOF >> /dev/stderr
usage: $0 options

This script launches a RADICAL-Pilot agent.

OPTIONS:
   -a      The name of project / allocation to charge.
   -b      name of sdist tarballs for radical stack install
   -c      Number of requested cores.
   -d      Specify debug level.
   -e      List of commands to run before bootstrapping.
   -f      Tunnel endpoint for connection forwarding.
   -g      Global shared virtualenv (create if missing)
   -h      Show this message.
   -i      The Python interpreter to use, e.g., python2.7.
   -j      Task launch method.
   -k      MPI launch method.
   -l      Type of Local Resource Management System.
   -m      Address and port of the coordination service host (MongoDB).
   -n      The name of the database.
   -o      The agent job spawning mechanism.
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
#   'recreate': delete if it exists, otherwise create, then use
#
# create and update ops will be locked and thus protected against concurrent
# bootstrapper invokations.
#
# (private + location in pilot sandbox == old behavior)
#
# That locking will likely not scale nicely for larger numbers of concurrent
# pilot, at least not for slow running updates (time for update of n pilots
# needs to be smaller than lock timeout).  OTOH, concurrent pip updates should
# not have a negative impact on the virtenv in the first place, AFAIU -- lock on
# create is more important, and should be less critical
#
virtenv_setup()
{
    profile_event 'virtenv_setup start'

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
        if ! test -d "$virtenv"
        then
            printf "\nERROR: given virtenv does not exists at $virtenv\n\n"
            unlock "$pid" "$virtenv"
            exit 1
        fi
        virtenv_create=FALSE
        virtenv_update=FALSE

    elif test "$virtenv_mode" = "recreate"
    then
        test -d "$virtenv" && rm -r "$virtenv"
        virtenv_create=TRUE
        virtenv_update=FALSE
    fi

    echo "virtenv_create   : $virtenv_create"
    echo "virtenv_update   : $virtenv_update"


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
                tar zxf $sdist
                RP_INSTALL_SOURCES="$RP_INSTALL_SOURCES $src/"
            done
            RP_INSTALL_TARGET="VIRTENV"
            ;;

        debug)
            for sdist in `echo $SDISTS | tr ':' ' '`
            do
                src=${sdist%.tgz}
                src=${sdist%.tar.gz}
                tar zxf $sdist
                RP_INSTALL_SOURCES="$RP_INSTALL_SOURCES $src/"
            done
            RP_INSTALL_TARGET="LOCAL"
            ;;

        release)
            RP_INSTALL_SOURCES='radical.pilot'
            RP_INSTALL_TARGET='VIRTENV'
            ;;

        installed)
            if test -d "$VIRTENV/rp_install"
            then
                RP_INSTALL_SOURCES=''
                RP_INSTALL_TARGET=''
            else
                echo "WARNING: 'rp_version' set to 'installed', "
                echo "         but no installed rp found in '$VIRTENV' ($virtenv_mode)"
                echo "         Settgins 'rp_version' to 'release'"
                RP_VERSION='release'
                RP_INSTALL_SOURCES='radical.pilot'
                RP_INSTALL_TARGET='VIRTENV'
            fi 
            ;;

        *)
            # do *not* use 'pip -e' -- egg linking does not work in PYTHONPATH.
            # Instead, we manually clone the respective git repository, and
            # switch to the respective branch/tag/commit.
            git clone https://github.com/radical-cybertools/radical.pilot.git
            (cd radical.pilot; git checkout $RP_VERSION)
            RP_INSTALL_SOURCES="radical.pilot/"
            RP_INSTALL_TARGET='VIRTENV'
    esac

    # NOTE: for any immutable virtenv (VIRTENV_MODE==use), we have to choose
    # a LOCAL install target.  LOCAL installation will only work with 'python
    # setup.py install', so we have to use the sdist -- the RP_INSTALL_SOURCES
    # has to point to directories
    if test "$virtenv_mode" = "use"
    then
        if test "$RP_INSTALL_TARGET" = "VIRTENV"
        then
            echo "WARNING: virtenv immutable - install RP locally"
            RP_INSTALL_TARGET='LOCAL'
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

    echo "rp install sources: $RP_INSTALL_SOURCES"
    echo "rp install target : $RP_INSTALL_TARGET"


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
    fi

    # creation or not -- at this point it needs activation
    virtenv_activate "$virtenv"


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
    fi

    # install RP
    rp_install "$RP_INSTALL_SOURCES" "$RP_INSTALL_TARGET"

    unlock "$pid" "$virtenv"

    profile_event 'virtenv_setup end'
}


# ------------------------------------------------------------------------------
#
virtenv_activate()
{
    if test "$VIRTENV_IS_ACTIVATED" = "TRUE"
    then
        return
    fi

    . "$VIRTENV/bin/activate"
    VIRTENV_IS_ACTIVATED=TRUE

    prefix="$VIRTENV/rp_install"

    python_version=`python --version 2>&1 | cut -f 2 -d ' ' | cut -f 1-2 -d .`
    mod_prefix="$prefix/lib/python$python_version/site-packages"

    PYTHONPATH="$mod_prefix:$PYTHONPATH"
    export PYTHONPATH

    PATH="$prefix/bin:$PATH"
    export PATH
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
    profile_event 'virtenv_create start'

    virtenv="$1"

    # NOTE: create a fresh virtualenv. we use an older 1.9.x version of
    # virtualenv as this seems to work more reliable than newer versions.
    # If we can't download, we try to move on with the system virtualenv.
    run_cmd "Download virtualenv tgz" \
            "curl -k -O '$VIRTENV_TGZ_URL'"

    if ! test "$?" = 0
    then
        echo "WARNING: Couldn't download virtualenv via curl! Using system version."
        BOOTSTRAP_CMD="virtualenv $virtenv"

    else :
        run_cmd "unpacking virtualenv tgz" \
                "tar xvfz '$VIRTENV_TGZ'"

        if test $? -ne 0
        then
            echo "Couldn't unpack virtualenv!"
            return 1
        fi

        BOOTSTRAP_CMD="$PYTHON virtualenv-1.9/virtualenv.py $virtenv"
    fi


    run_cmd "Create virtualenv" \
            "$BOOTSTRAP_CMD"
    if test $? -ne 0
    then
        echo "Couldn't create virtualenv"
        return 1
    fi

    # activate the virtualenv
    virtenv_activate "$virtenv"

  # run_cmd "Downgrade pip to 1.2.1" \
  #         "easy_install pip==1.2.1" \
  #      || echo "Couldn't downgrade pip! Using default version (if it exists)"


    run_cmd "update setuptools" \
            "$PIP install --upgrade setuptools" \
         || echo "Couldn't update setuptools -- using default version"

    run_cmd "update pip" \
            "$PIP install --upgrade pip" \
         || echo "Couldn't update pip -- using default version"


    # NOTE: On india/fg 'pip install saga-python' does not work as pip fails to
    # install apache-libcloud (missing bz2 compression).  We thus install that
    # dependency via easy_install.
    run_cmd "install apache-libcloud" \
            "easy_install --upgrade apache-libcloud" \
         || echo "Couldn't install/upgrade apache-libcloud! Lets see how far we get ..."


    # now that the virtenv is set up, we install all dependencies 
    # of the RADICAL stack
    for dep in "$VIRTENV_RADICAL_DEPS"
    do
        run_cmd "install $dep" \
                "$PIP install $dep" \
             || echo "Couldn't install $dep! Lets see how far we get ..."
    done
}


# ------------------------------------------------------------------------------
#
# update virtualenv - this assumes that the virtenv has been activated
#
virtenv_update()
{
    profile_event 'virtenv_update start'

    virtenv="$1"
    virtenv_activate "$virtenv"

    # we upgrade all dependencies of the RADICAL stack, one by one.
    # FIXME: for now, we only do pip upgrades -- that will ignore the
    # easy_installed modules on india (and posisbly other hosts).
    for dep in "$VIRTENV_RADICAL_DEPS"
    do
        run_cmd "install $dep" \
                "$PIP install --upgrade $dep" \
             || echo "Couldn't update $dep! Lets see how far we get ..."
    done

    profile_event 'virtenv_update done'
}


# ------------------------------------------------------------------------------
#
# Install the radical stack, ie. install RP which pulls the rest. 
# This assumes that the virtenv has been activated.  Any previously installed
# stack version is deleted.
#
# As the virtenv should have all dependencies set up (see VIRTENV_RADICA__DEPS),
# we don't expect any additional module pull from pypi.  Some rp_versions will, 
# however, pull the rp modules from pypi or git.
#
# . $VIRTENV/bin/activate
# rm -rf $VIRTENV/radical
#
# case rp_version:
#   @<token>:
#   @tag/@branch/@commit: # no sdist staging
#       git clone $github_base radical.pilot.src
#       (cd radical.pilot.src && git checkout token)
#       pip install -t $VIRTENV/radical/ radical.pilot.src
#       rm -rf radical.pilot.src
#       export PYTHONPATH=$VIRTENV/radical:$PYTHONPATH
#
#   release: # no sdist staging
#       pip install -t $VIRTENV/radical radical.pilot
#       export PYTHONPATH=$VIRTENV/radical:$PYTHONPATH
#
#   local: # needs sdist staging
#       tar zxf $sdist.tgz
#       pip install -t $VIRTENV/radical $sdist/
#       export PYTHONPATH=$VIRTENV/radical:$PYTHONPATH
#
#   debug: # needs sdist staging
#       tar zxf $sdist.tgz
#       pip install -t $SANDBOX/radical $sdist/
#       export PYTHONPATH=$SANDBOX/radical:$PYTHONPATH
#
#   installed: # no sdist staging
#       true
# esac
#
#
rp_install()
{
    rp_install_sources="$1"
    rp_install_target="$2"

    if test -z "$rp_install_target"
    then
        echo "no RP install target - skip install"
        return
    fi

    profile_event 'rp_install start'

    echo "Using RADICAL-Pilot install sources '$rp_install_sources'"

    # install into a mutable virtenv (no matter if that exists in
    # a local sandbox or elsewhere)
    case "$rp_install_target" in
    
        VIRTENV)
            prefix="$VIRTENV/rp_install"
            ;;

        LOCAL)
            prefix="$SANDBOX/rp_install"
            ;;

        *)
            # this should never happen
            echo "ERROR: invalid RP install target '$RP_INSTALL_TARGET'"
            exit 1
    esac

    rm -rf "$prefix"
    mkdir  "$prefix"

    python_version=`python --version 2>&1 | cut -f 2 -d ' ' | cut -f 1-2 -d .`
    mod_prefix="$prefix/lib/python$python_version/site-packages"

    if test "$rp_install_target" = "LOCAL"
    then
        # local PYTHONPATH needs to be pre-pended.  The ve PYTHONPATH is
        # already set during ve activation...
        # NOTE: PYTHONPATH is set differently than the 'prefix' used during
        #       install
        PYTHONPATH="$mod_prefix:$PYTHONPATH"
        export PYTHONPATH

        PATH="$prefix/bin:$PATH"
        export PATH
    fi

    # NOTE: we first uninstall RP (for some reason, 'pip install --upgrade' does
    #       not work with all source types
    run_cmd "uninstall radical.pilot" "$PIP uninstall -y radical.pilot"
    # ignore any errors

    # NOTE: we need to add the radical name __init__.py manually here --
    #      distutil is broken and will not install it.
    mkdir -p   "$mod_prefix/radical/"
    ru_ns_init="$mod_prefix/radical/__init__.py"
    echo                                              >  $ru_ns_init
    echo 'import pkg_resources'                       >> $ru_ns_init
    echo 'pkg_resources.declare_namespace (__name__)' >> $ru_ns_init
    echo                                              >> $ru_ns_init

    pip_flags="--upgrade"
    pip_flags="$pip_flags --src '$prefix/src'"
    pip_flags="$pip_flags --build '$prefix/build'"
    pip_flags="$pip_flags --install-option='--prefix=$prefix'"

    for src in $RP_INSTALL_SOURCES
    do
        run_cmd "update $src via pip" \
                "$PIP install $pip_flags $src"
        
        if test $? -ne 0
        then
            echo "Couldn't install $src! Lets see how far we get ..."
        fi

        # NOTE: why? fuck pip, that's why!
        rm -rf "$prefix/build"
    done

    profile_event 'rp_install done'
}


# ------------------------------------------------------------------------------
# Verify that we ended up with a usable installation.  This will also print all
# versions and module locations, which is nice for debugging...
#
verify_rp_install()
{
    OLD_SAGA_VERBOSE=$SAGA_VERBOSE
    OLD_RADICAL_VERBOSE=$RADICAL_VERBOSE
    OLD_RADICAL_PILOT_VERBOSE=$RADICAL_PILOT_VERBOSE
    
    SAGA_VERBOSE=WARNING
    RADICAL_VERBOSE=WARNING
    RADICAL_PILOT_VERBOSE=WARNING
    
    # print the ve information and stack versions for verification
    echo
    echo "---------------------------------------------------------------------"
    echo
    echo "`python --version` (`which python`)"
    echo "PYTHONPATH: $PYTHONPATH"
 (  python -c 'print "utils : ",; import radical.utils as ru; print ru.version_detail,; print ru.__file__' \
 && python -c 'print "saga  : ",; import saga          as rs; print rs.version_detail,; print rs.__file__' \
 && python -c 'print "pilot : ",; import radical.pilot as rp; print rp.version_detail,; print rp.__file__' \
 && (echo 'install ok!'; true) \
 ) \
 || (echo 'install failed!'; false) \
 || exit 1
    echo
    echo "---------------------------------------------------------------------"
    echo
    
    SAGA_VERBOSE=$OLD_SAGA_VERBOSE
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
# run a preprocess command -- and exit if it happens to fail
#
# preprocess commands are executed right in arg parser loop because -e can be
# passed multiple times
#
preprocess()
{
    cmd=$@
    run_cmd "Running pre-process command" "$cmd"

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
while getopts "a:b:c:D:d:e:f:g:hi:j:k:l:m:n:o:p:q:r:u:s:t:v:w:x:y:z:" OPTION; do
    case $OPTION in
        a)  AUTH=$OPTARG  ;;
        b)  SDISTS=$OPTARG  ;;
        c)  CORES=$OPTARG  ;;
        D)  TUNNEL_BIND_DEVICE=$OPTARG ;;
        d)  DEBUG=$OPTARG  ;;
        e)  preprocess "$OPTARG"  ;;
        f)  FORWARD_TUNNEL_ENDPOINT=$OPTARG  ;;
        g)  VIRTENV=$(eval echo $OPTARG)  ;;
        i)  PYTHON=$OPTARG  ;;
        j)  TASK_LAUNCH_METHOD=$OPTARG  ;;
        k)  MPI_LAUNCH_METHOD=$OPTARG  ;;
        l)  LRMS=$OPTARG  ;;
        m)  DBURL=$OPTARG   ;;
        n)  DBNAME=$OPTARG  ;;
        o)  SPAWNER=$OPTARG  ;;
        p)  PILOT_ID=$OPTARG  ;;
        q)  SCHEDULER=$OPTARG  ;;
        r)  RUNTIME=$OPTARG  ;;
        s)  SESSIONID=$OPTARG  ;;
        t)  AGENT_TYPE=$OPTARG  ;;
        u)  VIRTENV_MODE=$OPTARG  ;;
        v)  RP_VERSION=$OPTARG  ;;
        w)  SANDBOX=$OPTARG  ;;
        x)  CLEANUP=$OPTARG  ;;
        *)  usage "Unknown option: $OPTION=$OPTARG"  ;;
    esac
done

# Check that mandatory arguments are set
# (Currently all that are passed through to the agent)
if test -z "$CORES"              ; then  usage "missing CORES             ";  fi
if test -z "$DEBUG"              ; then  usage "missing DEBUG             ";  fi
if test -z "$DBNAME"             ; then  usage "missing DBNAME            ";  fi
if test -z "$DBURL"              ; then  usage "missing DBURL             ";  fi
if test -z "$LRMS"               ; then  usage "missing LRMS              ";  fi
if test -z "$MPI_LAUNCH_METHOD"  ; then  usage "missing MPI_LAUNCH_METHOD ";  fi
if test -z "$SPAWNER"            ; then  usage "missing SPAWNER           ";  fi
if test -z "$PILOT_ID"           ; then  usage "missing PILOT_ID          ";  fi
if test -z "$RUNTIME"            ; then  usage "missing RUNTIME           ";  fi
if test -z "$SCHEDULER"          ; then  usage "missing SCHEDULER         ";  fi
if test -z "$SESSIONID"          ; then  usage "missing SESSIONID         ";  fi
if test -z "$TASK_LAUNCH_METHOD" ; then  usage "missing TASK_LAUNCH_METHOD";  fi
if test -z "$RP_VERSION"         ; then  usage "missing RP_VERSION        ";  fi

# If the host that will run the agent is not capable of communication
# with the outside world directly, we will setup a tunnel.
if [[ $FORWARD_TUNNEL_ENDPOINT ]]; then

    profile_event 'tunnel setup start'

    echo "# -------------------------------------------------------------------"
    echo "# Setting up forward tunnel for MongoDB to $FORWARD_TUNNEL_ENDPOINT."

    # Bind to localhost
    BIND_ADDRESS=`/sbin/ifconfig $TUNNEL_BIND_DEVICE|grep "inet addr"|cut -f2 -d:|cut -f1 -d" "`

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
    FORWARD_TUNNEL_ENDPOINT_HOST=$FORWARD_TUNNEL_ENDPOINT
    ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:$DBURL -p $FORWARD_TUNNEL_ENDPOINT_PORT $FORWARD_TUNNEL_ENDPOINT_HOST &

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

# NOTE: if a cacert.pem.gz was staged, we unpack it and use it for all pip
# commands.  Its a sign that the pip cacert (or the system's, dunno) is not up
# to date.  Easy_install seems to use a different access channel, so does not
# need the cert bundle.
#
if test -f 'cacert.pem.gz'
then
    gunzip cacert.pem.gz
    PIP='pip --cert cacert.pem'
else
    PIP='pip'
fi


# ready to setup the virtenv
virtenv_setup    "$PILOT_ID" "$VIRTENV" "$VIRTENV_MODE"
virtenv_activate "$VIRTENV"

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
# NOTE: For some reasons, I have seen installations where 'scripts' go into
# bin/, and some where setuptools only changes them in place.  For now, we allow
# for both -- but eventually (once the agent itself is small), we may want to
# move it to bin ourself...
if test "$RP_INSTALL_TARGET" = 'LOCAL'
then
    PILOT_SCRIPT="$SANDBOX/rp_install/bin/radical-pilot-agent-${AGENT_TYPE}.py"
    if ! test -e "$PILOT_SCRIPT"
    then
        PILOT_SCRIPT="$SANDBOX/rp_install/lib/python$python_version/site-packages/radical/pilot/agent/radical-pilot-agent-${AGENT_TYPE}.py"
    fi
else
    PILOT_SCRIPT="$VIRTENV/rp_install/bin/radical-pilot-agent-${AGENT_TYPE}.py"
    if ! test -e "$PILOT_SCRIPT"
    then
        PILOT_SCRIPT="$VIRTENV/rp_install/lib/python$python_version/site-packages/radical/pilot/agent/radical-pilot-agent-${AGENT_TYPE}.py"
    fi
fi

AGENT_CMD="python $PILOT_SCRIPT \
-c $CORES \
-d $DEBUG \
-j $TASK_LAUNCH_METHOD \
-k $MPI_LAUNCH_METHOD \
-l $LRMS \
-m $DBURL \
-n $DBNAME \
-o $SPAWNER \
-p $PILOT_ID \
-q $SCHEDULER \
-s $SESSIONID \
-r $RUNTIME"

if ! test -z "$AUTH"
then
    AGENT_CMD="$AGENT_CMD -a $AUTH"
fi

if test "$LRMS" = "CCM"
then
    AGENT_CMD="ccmrun $AGENT_CMD"
fi

verify_rp_install

echo
echo "# -------------------------------------------------------------------"
echo "# Launching radical-pilot-agent for $CORES cores."
echo "# CMDLINE: $AGENT_CMD"

# enable DebugHelper in agent
export RADICAL_DEBUG=TRUE

export SAGA_VERBOSE=DEBUG
export RADIAL_VERBOSE=DEBUG
export RADIAL_UTIL_VERBOSE=DEBUG
export RADIAL_PILOT_VERBOSE=DEBUG

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
contains $CLEANUP 'l' && rm -r "$SANDBOX/agent.*"
contains $CLEANUP 'u' && rm -r "$SANDBOX/unit.*"
contains $CLEANUP 'v' && rm -r "$VIRTENV/"
contains $CLEANUP 'e' && rm -r "$SANDBOX/"

profile_event 'cleanup done'
echo "#"
echo "# -------------------------------------------------------------------"

# ... and exit
exit $AGENT_EXITCODE

