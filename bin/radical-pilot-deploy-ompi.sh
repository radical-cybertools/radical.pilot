#!/bin/sh

# this script performs an OpenMPI installation which should be suitable to use
# with radical.pilot.  The default target location is $HOME/ompi, which will
# include subdirs for source, installation and module files.
#
# Note that the installation tree needs to be visible on compute nodes in order
# to be usabel by radical.pilot.
#
# Note further that this includes the installation of various parts of the
# compilation toolchain which might actually already exist on the system -- we
# make no attempt to use system tools at this point, for the sake of simplicity
# and stability.  For any experienced user / admin, this might be easy to
# resolve though.
#
# Thanks to Mark Santcroos to provide the input for this installation
# procedure!

# export OMPI_COMMIT=a3ac67be0d
# export OMPI_COMMIT=539f71d     # last working on titan
# export OMPI_COMMIT=master
# export OMPI_COMMIT=64e838c1ac
# export OMPI_COMMIT=7839dc91a8
# export OMPI_COMMIT=a76a61b2c9
# export OMPI_COMMIT=d9b2c94
# export OMPI_COMMIT=47bf0d6f9d
# export OMPI_COMMIT=e88767866e
# export OMPI_COMMIT=51f3fbdb3e  # Fix cmd line passing of DVM URI   Oct 6 2017
# export OMPI_COMMIT=04ec013da9  # 04.03.2018
# export OMPI_COMMIT=e9f378e851

if (hostname -f | grep titan)
then
    echo "configure for Titan"

    export OMPI_DIR=/lustre/atlas2/csc230/world-shared/openmpi/
    export OMPI_COMMIT=539f71d # last working on titan

    module load   python
    module load   python_pip
    module load   python_virtualenv

    # for openmpi build
    module unload PrgEnv-pgi || true
    module load   PrgEnv-gnu || true
    module unload cray-mpich || true
#   module load   torque
    module load   pmi
    # ./configure --prefix=/lustre/atlas2/csc230/world-shared/openmpi/src/ompi../../install/test/ --enable-debug --enable-timing --enable-heterogeneous --enable-mpi-cxx --enable-install-libpmix --enable-pmix-timing --with-pmix=internal --with-ugni --with-cray-pmi --with-alps=yes --with-tm 
  
    # load what was installed above
  # module use --append /lustre/atlas2/csc230/world-shared/openmpi/modules
  # module load openmpi/test

    # for charm build
  # module load   cudatoolkit   # problems with UCL workload?
    module load   cmake
   ## ./build charm++ mpi-linux-x86_64 mpicxx smp omp pthreads -j16
    # ./build charm++ mpi-linux-x86_64 mpicxx -j16

    # for namd build
    module load   rca
   ## ./config CRAY-XE-gnu --charm-base ../charm-openmpi/ --charm-arch mpi-linux-x86_64-omp-pthreads-smp-mpicxx --fftw-prefix /lustre/atlas2/csc230/world-shared/openmpi/applications/namd/namd-openmpi/../fftw-2.1.5/install --with-tcl
    # ./config CRAY-XE-gnu --charm-base ../charm-openmpi/ --charm-arch -linux-x86_64-mpicxx --fftw-prefix /lustre/atlas2/csc230/world-shared/openmpi/applications/namd/fftw-2.1.5/install --with-tcl


elif (hostname | grep h2o) 
then
    echo "configure for BW"

    export OMPI_DIR=/projects/sciteam/bamm/merzky/openmpi/
    export OMPI_COMMIT=539f71d # last working on titan

    if test -z "$BWPY_VERSION"
    then
        echo "bwpy module not loaded"
        exit -1
    fi

    module unload cray-mpich  || true
    module unload PrgEnv-cray || true
    module load   PrgEnv-gnu  || true
    module load   torque
    module load   cmake

else
    echo 'configure for localhost'
    export OMPI_DIR=$HOME/radical/ompi/
    export OMPI_COMMIT=HEAD
fi



export OMPI_LABEL=test
export OMPI_LABEL=$(date '+%Y_%m_%d'_${OMPI_COMMIT}) # module flag for installed version
export MAKEFLAGS=-j32                                # speed up build on multicore machines

if ! test -z "$1"
then
    export OMPI_DIR="$1"
fi

LOG="$OMPI_DIR/ompi.$OMPI_LABEL.deploy.log"
rm -f "$LOG"

echo "------------------------------"
echo "ompi dir: $OMPI_DIR"
echo "ompi log: $LOG"
echo

log(){
    msg=$1
    printf "%-10s : " "$msg"
    while read in
    do
        echo "$in" >> "$LOG"
        echo "$in" | sed -e 's/[^\n]//g' | sed -e 's/.*/./g' | xargs echo -n
    done
    echo
}


# The environments below are only important during build time
# and can generally point anywhere on the filesystem.

orig=$(pwd)
export OMPI_DOWNLOAD=$OMPI_DIR/download
export OMPI_SOURCE=$OMPI_DIR/src
export OMPI_TOOLS_PREFIX=$OMPI_DIR/tools
export PATH=$OMPI_TOOLS_PREFIX/bin:$PATH

# The file system locations of the variables below need a bit more care,
# as this path needs to be accessible during job run time.
# E.g. on Blue Waters, a good location for
# OMPI_INSTALLED=/projects/sciteam/gk4/openmpi/installed,
# and OMPI_MODULE_BASE=/projects/sciteam/gk4/openmpi/modules.

export OMPI_MODULE_BASE=$OMPI_DIR/modules
export OMPI_MODULE=$OMPI_MODULE_BASE/openmpi
export OMPI_INSTALLED=$OMPI_DIR/installed

echo $OMPI_DIR/$OMPI_LABEL

mkdir -p $OMPI_DOWNLOAD
mkdir -p $OMPI_SOURCE


# ------------------------------------------------------------------------------
deps="help2man-1.43.3 autoconf-2.69 automake-1.16 libtool-2.4.2 m4-1.4.16"
for dep in $deps
do
    echo "install $dep"
    gnu='http://ftp.gnu.org/gnu'
    dbase=$(echo $dep | cut -f 1 -d '-')
    dsrc=$dep.tar.gz
    cd $OMPI_DOWNLOAD
    if ! test -f $dsrc
    then
        wget $gnu/$dbase/$dep.tar.gz            2>&1 | log 'wget' || exit
    fi
    cd $OMPI_SOURCE
    if ! test -d $dep
    then
        tar xvf $OMPI_DOWNLOAD/$dep.tar.gz      2>&1 | log 'tar ' || exit
        cd $dep
        ./configure --prefix=$OMPI_TOOLS_PREFIX 2>&1 | log 'cfg ' || exit
        make                                    2>&1 | log 'make' || exit
        make install                            2>&1 | log 'inst' || exit
    else
        echo '  skipped'
    fi
    echo
done

# ------------------------------------------------------------------------------
# install libffi on systems which don't have it, so that the pilot ve can
# install the `orte_cffi` python module.
# libffi documentation needs texi2html which is not commonly available, so we
# disable documentation.

echo "install libffi"
cd $OMPI_SOURCE
if ! test -d libffi
then
    git clone https://github.com/libffi/libffi.git \
                 2>&1 | log 'git ' || exit
    cd libffi
    git pull     2>&1 | log 'pull' || exit
    ./autogen.sh 2>&1 | log 'agen' || exit
    ./configure --prefix=$OMPI_TOOLS_PREFIX --disable-docs \
                 2>&1 | log 'cfg ' || exit
    make         2>&1 | log 'make' || exit
    make install 2>&1 | log 'inst' || exit
fi

# ------------------------------------------------------------------------------
echo "install ompi @$OMPI_COMMIT"
cd $OMPI_SOURCE
if ! test -d ompi
then
    git clone https://github.com/open-mpi/ompi.git 2>&1 | log 'git ' || exit
fi
cd ompi

git checkout master        
git pull                          2>&1 | log 'pull' || exit
git checkout $OMPI_COMMIT         2>&1 | log 'comm' || exit
make distclean                    2>&1 | log 'clr '
test -f configure || ./autogen.pl 2>&1 | log 'agen' || exit

export OMPI_BUILD=$OMPI_DIR/build/$OMPI_LABEL
mkdir -p $OMPI_BUILD
cd $OMPI_BUILD
export CFLAGS=-O3
export CXXFLAGS=-O3
export FCFLAGS="-ffree-line-length-none"
echo "-----------------------------------------"
echo "OMPI_DIR      : $OMPI_DIR"
echo "OMPI_SOURCE   : $OMPI_SOURCE"
echo "OMPI_BUILD    : $OMPI_BUILD"
echo "OMPI_INSTALLED: $OMPI_INSTALLED"
echo "OMPI_LABEL    : $OMPI_LABEL"

echo "modules       :"  | log 'mods'
module list 2>&1 | sort | log 'mods'

echo $PATH
echo "-----------------------------------------"

# titan
dummy="
     --with-ugni

     --with-cray-pmi
     --enable-pmix-timing

     --with-pmix=internal
     --enable-install-libpmix

     --with-alps=no
     --with-tm

     --with-cray-pmi
     --with-pmi=/opt/cray/pmi/5.0.14/

     --enable-mpi-cxx
     --enable-timing
     --enable-heterogeneous
"
# export LDFLAGS="-lpmi -L/opt/cray/pmi/5.0.14/lib64"
cfg="$OMPI_SOURCE/ompi/configure
     --prefix=$OMPI_INSTALLED/$OMPI_LABEL
     --with-devel-headers
     --disable-debug
     --enable-static
     --enable-orterun-prefix-by-default
"

$cfg          2>&1 | log 'cfg ' || exit
make -j 32    2>&1 | log 'make' || exit
make install  2>&1 | log 'inst' || exit
echo "$cfg" > $OMPI_INSTALLED/$OMPI_LABEL/cfg.log
env         > $OMPI_INSTALLED/$OMPI_LABEL/env.log

# ------------------------------------------------------------------------------
echo "create module file"
mkdir -p $OMPI_MODULE
cat <<EOT > $OMPI_MODULE/$OMPI_LABEL
#%Module########################################################################

##
## Open MPI from git
##
proc ModulesHelp { } {
        global version

        puts stderr "Sets up a dynamic build of Open MPI HEAD from git."
        puts stderr "Version $OMPI_LABEL @$OMPI_COMMIT"
}

module-whatis "Dynamic build of Open MPI from git."

set version $OMPI_LABEL

prepend-path    PATH            $OMPI_INSTALLED/$OMPI_LABEL/bin
prepend-path    LD_LIBRARY_PATH $OMPI_INSTALLED/$OMPI_LABEL/lib
prepend-path    MANPATH         $OMPI_INSTALLED/$OMPI_LABEL/share/man
prepend-path    PKG_CONFIG_PATH $OMPI_INSTALLED/$OMPI_LABEL/share/pkgconfig

setenv          OMPI_MCA_timer_require_monotonic false

EOT

cat <<EOT > $OMPI_INSTALLED/$OMPI_LABEL/etc/ompi.sh
export PATH=\$PATH:$OMPI_INSTALLED/$OMPI_LABEL/bin
export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$OMPI_INSTALLED/$OMPI_LABEL/lib
export MANPATH=\$MANPATH:$OMPI_INSTALLED/$OMPI_LABEL/share/man
export PKG_CONFIG_PATH=\$PKG_CONFIG_PATH:$OMPI_INSTALLED/$OMPI_LABEL/share/pkgconfig
export OMPI_MCA_timer_require_monotonic=false
EOT
echo


# ------------------------------------------------------------------------------
# activate settings for this scipt
. $OMPI_INSTALLED/$OMPI_LABEL/etc/ompi.sh


# ------------------------------------------------------------------------------
echo
gver=gromacs-2018.2
gsrc=$gver.tar.gz
echo "install gromacs $gver"

cd $OMPI_DOWNLOAD
if ! test -f $gsrc
then
    wget http://ftp.gromacs.org/pub/gromacs/$gsrc \
            2>&1 | log 'wget' || exit
fi

cd $OMPI_SOURCE
if ! test -d $gver
then
    tar xf $OMPI_DOWNLOAD/$gver.tar.gz 2>&1 | log 'wget' || exit
    cd $gver
    
    # module use --append $OMPI_MODULE_BASE
    # module load openmpi/$OMPI_LABEL
    # module list
    
    cmake \
      -DCMAKE_C_COMPILER=mpicc \
      -DCMAKE_CXX_COMPILER=mpiCC \
      -DGMX_MPI=on \
      -DCMAKE_INSTALL_PREFIX=$OMPI_INSTALLED/$OMPI_LABEL \
      -DBUILD_SHARED_LIBS=ON \
      -DGMX_BUILD_OWN_FFTW=ON \
      -DGMX_OPENMP=OFF \
      -DGMX_SIMD=AVX_128_FMA \
                 2>&1 | log 'cmak' || exit
    make         2>&1 | log 'make' || exit
    make install 2>&1 | log 'inst' || exit
fi

# this should not be needed - but just in case someone sources this script,
# we try to end up where we started.
cd $orig

echo "installed $OMPI_INSTALLED/$OMPI_LABEL"
# ------------------------------------------------------------------------------

