#!/bin/bash -l

# ------------------------------------------------------------------------------
# Copyright 2013-2015, RADICAL @ Rutgers
# Licensed under the MIT License
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
# Arguments passed to the bootstrapper should be required by the bootstrapper
# itself, and not only to be passed down to the agent.
# Configuration only used by the agent should go in the agent config file,
# and not be passed as an argument to the bootstrapper.
# Configuration used by both should be passed to the bootstrapper and
# consecutively passed to the agent. Only in cases where it is justified, that
# information can be "duplicated" in the agent config.
# The rationale for this is:
# 1) that the bootstrapper can't (easily) read from MongoDB, so it needs to
#    to get the information as arguments;
# 2) that the agent needs information that goes beyond what can be put in
#    arguments, both qualitative and quantitatively.
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

# flag which is set when a system level RP installation is found, triggers
# '--upgrade' flag for pip
# NOTE: this mechanism is disabled, as it affects a minority of machines and
#       adds too much complexity for too little benefit.  Also, it will break on
#       machines where pip has no connectivity, and pip cannot silently ignore
#       that system version...
# SYSTEM_RP='FALSE'


# seconds to wait for lock files
# 3 min should be enough for anybody to create/update a virtenv...
LOCK_TIMEOUT=180 # 3 min
VIRTENV_TGZ_URL="https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz"
VIRTENV_TGZ="virtualenv-1.9.tar.gz"
VIRTENV_IS_ACTIVATED=FALSE
VIRTENV_RADICAL_DEPS="pymongo==2.8 apache-libcloud colorama python-hostlist"


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
PROFILE_LOG="agent.prof"
profile_event()
{
    if ! test -z "$RADICAL_PILOT_PROFILE"
    then
        timestamp
        NOW=$((TIMESTAMP-TIME_ZERO))
        # Format: time, component, uid, event, message"
        printf "%.4f,%s,%s,%s,%s\n" \
            "$NOW" "Bootstrapper" "$PILOT_ID" "$@" "" >> $PROFILE_LOG
    fi
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
    if test -f 'cacert.pem.gz'
    then
        gunzip cacert.pem.gz
    fi

    if test -f 'cacert.pem'
    then
        PIP="`which pip` --cert cacert.pem"
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
    eval "$cmd"
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
        if test -f "$virtenv/bin/activate"
        then
            printf "\nERROR: private virtenv already exists at $virtenv\n\n"
            unlock "$pid" "$virtenv"
            exit 1
        fi
        virtenv_create=TRUE
        virtenv_update=FALSE

    elif test "$virtenv_mode" = "update"
    then
        test -f "$virtenv/bin/activate" || virtenv_create=TRUE
        virtenv_update=TRUE

    elif test "$virtenv_mode" = "create"
    then
        virtenv_create=TRUE
        virtenv_update=FALSE

    elif test "$virtenv_mode" = "use"
    then
        if ! test -f "$virtenv/bin/activate"
        then
            printf "\nERROR: given virtenv does not exists at $virtenv\n\n"
            unlock "$pid" "$virtenv"
            exit 1
        fi
        virtenv_create=FALSE
        virtenv_update=FALSE

    elif test "$virtenv_mode" = "recreate"
    then
        test -f "$virtenv/bin/activate" && rm -r "$virtenv"
        virtenv_create=TRUE
        virtenv_update=FALSE
    else
        printf "\nERROR: virtenv mode invalid: $virtenv_mode\n\n"
        unlock "$pid" "$virtenv"
        exit 1
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
                tar zxmf $sdist
                RP_INSTALL_SOURCES="$RP_INSTALL_SOURCES $src/"
            done
            RP_INSTALL_TARGET='VIRTENV'
            RP_INSTALL_SDIST='TRUE'
            ;;

        debug)
            for sdist in `echo $SDISTS | tr ':' ' '`
            do
                src=${sdist%.tgz}
                src=${sdist%.tar.gz}
                tar zxmf $sdist
                RP_INSTALL_SOURCES="$RP_INSTALL_SOURCES $src/"
            done
            RP_INSTALL_TARGET='SANDBOX'
            RP_INSTALL_SDIST='TRUE'
            ;;

        release)
            RP_INSTALL_SOURCES='radical.pilot'
            RP_INSTALL_TARGET='VIRTENV'
            RP_INSTALL_SDIST='FALSE'
            ;;

        installed)
            if test -d "$VIRTENV/rp_install"
            then
                RP_INSTALL_SOURCES=''
                RP_INSTALL_TARGET=''
                RP_INSTALL_SDIST=''
            else
                echo "WARNING: 'rp_version' set to 'installed', "
                echo "         but no installed rp found in '$VIRTENV' ($virtenv_mode)"
                echo "         Setting 'rp_version' to 'release'"
                RP_VERSION='release'
                RP_INSTALL_SOURCES='radical.pilot'
                RP_INSTALL_TARGET='VIRTENV'
                RP_INSTALL_SDIST='FALSE'
            fi 
            ;;

        *)
            # NOTE: do *not* use 'pip -e' -- egg linking does not work with 
            #       PYTHONPATH.  Instead, we manually clone the respective 
            #       git repository, and switch to the branch/tag/commit.
            git clone https://github.com/radical-cybertools/radical.pilot.git
            (cd radical.pilot; git checkout $RP_VERSION)
            RP_INSTALL_SOURCES="radical.pilot/"
            RP_INSTALL_TARGET='VIRTENV'
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

    echo "rp install sources: $RP_INSTALL_SOURCES"
    echo "rp install target : $RP_INSTALL_TARGET"


    # create virtenv if needed.  This also activates the virtenv.
    if test "$virtenv_create" = "TRUE"
    then
        if ! test -f "$virtenv/bin/activate"
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
    rp_install "$RP_INSTALL_SOURCES" "$RP_INSTALL_TARGET" "$RP_INSTALL_SDIST"

    unlock "$pid" "$virtenv"

    profile_event 'virtenv_setup end'
}


# ------------------------------------------------------------------------------
#
virtenv_activate()
{
    virtenv="$1"

    if test "$VIRTENV_IS_ACTIVATED" = "TRUE"
    then
        return
    fi

    . "$virtenv/bin/activate"
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
    PYTHON_VERSION=`python -c 'import distutils.sysconfig as sc; print sc.get_python_version()'`
    VE_MOD_PREFIX=` python -c 'import distutils.sysconfig as sc; print sc.get_python_lib()'`
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

    # NOTE: this should not be necessary, but we explicit set PYTHONPATH to
    #       include the VE module tree, because some systems set a PYTHONPATH on
    #       'module load python', and that would supersede the VE module tree,
    #       leading to unusable versions of setuptools.
    PYTHONPATH="$VE_MOD_PREFIX:$VE_PYTHONPATH"
    export PYTHONPATH

    echo "activated virtenv"
    echo "VIRTENV      : $virtenv"
    echo "VE_MOD_PREFIX: $VE_MOD_PREFIX"
    echo "RP_MOD_PREFIX: $RP_MOD_PREFIX"
    echo "PYTHONPATH   : $PYTHONPATH"
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

    # NOTE: create a fresh virtualenv. We use an older 1.9.x version of
    #       virtualenv as this seems to work more reliable than newer versions.
    run_cmd "Download virtualenv tgz" \
            "curl -k -O '$VIRTENV_TGZ_URL'"

    if ! test "$?" = 0
    then
        echo "WARNING: Couldn't download virtualenv via curl! Using system version."
        BOOTSTRAP_CMD="virtualenv $virtenv"

    else :
        run_cmd "unpacking virtualenv tgz" \
                "tar zxmf '$VIRTENV_TGZ'"

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

    # make sure we have pip
    PIP=`which pip`
    if test -z "$PIP"
    then
        run_cmd "install pip" \
                "easy_install pip" \
             || echo "Couldn't install pip! Uh oh...."
    fi

    # NOTE: setuptools 15.0 (which for some reason is the next release afer
    #       0.6c11) breaks on BlueWaters, and breaks badly (install works, but
    #       pip complains about some parameter mismatch).  So we fix on the last
    #       known workable version -- which seems to be acceptable to other
    #       hosts, too
    run_cmd "update setuptools" \
            "$PIP install --upgrade setuptools==0.6c11" \
         || echo "Couldn't update setuptools -- using default version"


    # NOTE: new releases of pip deprecate options we depend upon.  While the pip
    #       developers discuss if those options will get un-deprecated again,
    #       fact is that there are released pip versions around which do not 
    #       work for us (hello supermuc!).  So we fix the version to one we know
    #       is functional.
    run_cmd "update pip" \
            "$PIP install --upgrade pip==1.4.1" \
         || echo "Couldn't update pip -- using default version"

    # make sure the new pip version is used (but keep the python executable)
    rehash "$PYTHON"


    # NOTE: On india/fg 'pip install saga-python' does not work as pip fails to
    #       install apache-libcloud (missing bz2 compression).  We thus install 
    #       that dependency via easy_install.
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
    # NOTE: we only do pip upgrades -- that will ignore the easy_installed 
    #       modules on india etc.
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
#       pip install -t $VIRTENV/rp_install/ radical.pilot.src
#       rm -rf radical.pilot.src
#       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
#
#   release: # no sdist staging
#       pip install -t $VIRTENV/rp_install radical.pilot
#       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
#
#   local: # needs sdist staging
#       tar zxmf $sdist.tgz
#       pip install -t $VIRTENV/rp_install $sdist/
#       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
#
#   debug: # needs sdist staging
#       tar zxmf $sdist.tgz
#       pip install -t $SANDBOX/rp_install $sdist/
#       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
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

    profile_event 'rp_install start'

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
            RP_INSTALL="$SANDBOX/rp_install"

            # make sure the lib path into the prefix conforms to the python conventions
            RP_LOC_PREFIX=`echo $VE_MOD_PREFIX | sed -e "s|$VIRTENV|$SANDBOX/rp_install|"`

            echo "VE_MOD_PREFIX: $VE_MOD_PREFIX"
            echo "VIRTENV      : $VIRTENV"
            echo "SANDBOX      : $SANDBOX"
            echo "VE_LOC_PREFIX: $VE_LOC_PREFIX"

            # local PYTHONPATH needs to be pre-pended.  The ve PYTHONPATH is
            # already set during ve activation -- but we don't want the rp_install
            # portion from that ve...
            # NOTE: PYTHONPATH is set differently than the 'prefix' used during
            #       install
            PYTHONPATH="$RP_LOC_PREFIX:$VE_MOD_REFIX:$VE_PYTHONPATH"
            export PYTHONPATH

            PATH="$SANDBOX/rp_install/bin:$PATH"
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

    # NOTE: we need to add the radical name __init__.py manually here --
    #       distutil is broken and will not install it.
    mkdir -p   "$RADICAL_MOD_PREFIX/"
    ru_ns_init="$RADICAL_MOD_PREFIX/__init__.py"
    echo                                              >  $ru_ns_init
    echo 'import pkg_resources'                       >> $ru_ns_init
    echo 'pkg_resources.declare_namespace (__name__)' >> $ru_ns_init
    echo                                              >> $ru_ns_init
    echo "created radical namespace in $RADICAL_MOD_PREFIX/__init__.py"

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

    pip_flags="$pip_flags --src '$SANDBOX/rp_install/src'"
    pip_flags="$pip_flags --build '$SANDBOX/rp_install/build'"
    pip_flags="$pip_flags --install-option='--prefix=$RP_INSTALL'"

    for src in $RP_INSTALL_SOURCES
    do
        run_cmd "update $src via pip" \
                "$PIP install $pip_flags $src"
        
        if test $? -ne 0
        then
            echo "Couldn't install $src! Lets see how far we get ..."
        fi

        # NOTE: why? fuck pip, that's why!
        rm -rf "$SANDBOX/rp_install/build"
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
    echo "`$PYTHON --version` ($PYTHON)"
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
env | sort
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

# FIXME: By now the pre_process rules are already performed.
#        We should split the parsing and the execution of those.
#        "bootstrap start" is here so that $PILOT_ID is known.
# Create header for profile log
if ! test -z "$RADICAL_PILOT_PROFILE"
then
    echo "time,component,uid,event,message" > $PROFILE_LOG
    profile_event 'bootstrap start'
fi

# NOTE: if the virtenv path contains a symbolic link element, then distutil will
#       report the absolute representation of it, and thus report a different
#       module path than one would expect from the virtenv path.  We thus
#       normalize the virtenv path before we use it.
mkdir -p "$VIRTENV"
echo "VIRTENV : $VIRTENV"
VIRTENV=`(cd $VIRTENV; pwd -P)`
echo "VIRTENV : $VIRTENV (normalized)"

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
    if test "$FORWARD_TUNNEL_ENDPOINT" = "BIND_ADDRESS"; then
        # On some systems, e.g. Hopper, sshd on the mom node is not bound to 127.0.0.1
        # In those situations, and if configured, bind to the just obtained bind address.
        FORWARD_TUNNEL_ENDPOINT_HOST=$BIND_ADDRESS
    else
        FORWARD_TUNNEL_ENDPOINT_HOST=$FORWARD_TUNNEL_ENDPOINT
    fi
    ssh -o StrictHostKeyChecking=no -x -a -4 -T -N -L $BIND_ADDRESS:$DBPORT:$DBURL -p $FORWARD_TUNNEL_ENDPOINT_PORT $FORWARD_TUNNEL_ENDPOINT_HOST &

    # Kill ssh process when bootstrapper dies, to prevent lingering ssh's
    trap 'jobs -p | xargs kill' EXIT

    # Overwrite DBURL
    DBURL=$BIND_ADDRESS:$DBPORT

    profile_event 'tunnel setup done'

fi

rehash "$PYTHON"

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
#       bin/, and some where setuptools only changes them in place.  For now, 
#       we allow for both -- but eventually (once the agent itself is small), 
#       we may want to move it to bin ourself.  At that point, we probably
#       have re-implemented pip... :/
# FIXME: the second option should use $RP_MOD_PATH, or should derive the path
#       from the imported rp modules __file__.
if test "$RP_INSTALL_TARGET" = 'SANDBOX'
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

