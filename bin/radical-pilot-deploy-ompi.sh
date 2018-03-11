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

# export OMPI_DIR=$HOME/ompi/                          # target location for install
export OMPI_DIR=/lustre/atlas2/csc230/world-shared/openmpi
export OMPI_COMMIT=539f71d                           # OpenMPI commit to install
export OMPI_COMMIT=master
export OMPI_COMMIT=64e838c1ac
export OMPI_COMMIT=7839dc91a8
export OMPI_COMMIT=a76a61b2c9
export OMPI_COMMIT=d9b2c94
export OMPI_COMMIT=47bf0d6f9d
export OMPI_COMMIT=e88767866e
export OMPI_COMMIT=51f3fbdb3e  # Fix cmd line passing of DVM URI   Oct 6 2017
export OMPI_COMMIT=04ec013da9  # 04.03.2018
export OMPI_LABEL=test
export OMPI_LABEL=$(date '+%Y_%m_%d'_${OMPI_COMMIT}) # module flag for installed version
export MAKEFLAGS=-j32                                # speed up build on multicore machines


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

TEST=$(hostname -f | grep titan)
if ! test -z "$TEST"
then
    module load   python
    module load   python_pip
    module load   python_virtualenv

    # for openmpi build
    module unload PrgEnv-pgi || true
    module load   PrgEnv-gnu || true
    module unload cray-mpich || true
    module load   torque
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
fi

mkdir -p $OMPI_DOWNLOAD
mkdir -p $OMPI_SOURCE

# wget http://ftp.nluug.nl/gnu/autoconf/autoconf-2.69.tar.gz
# wget http://ftp.nluug.nl/gnu/automake/automake-1.13.4.tar.gz
# wget http://nl.mirror.babylon.network/gnu/libtool/libtool-2.4.2.tar.gz
#    wget https://ftp.gnu.org/gnu/m4/m4-1.4.18.tar.gz
# wget ftp://ftp.gromacs.org/pub/gromacs/gromacs-5.1.4.tar.gz
# 
# cd $OMPI_SOURCE
# tar -xvzf $OMPI_DOWNLOAD/m4-1.4.18.tar.gz
# cd m4-1.4.18
# ./configure --prefix=$OMPI_TOOLS_PREFIX
# make
# make install
# 
# cd $OMPI_SOURCE
# tar -xvzf $OMPI_DOWNLOAD/autoconf-2.69.tar.gz
# cd autoconf-2.69
# ./configure --prefix=$OMPI_TOOLS_PREFIX
# make
# make install
# 
# cd $OMPI_SOURCE
# tar -xvzf $OMPI_DOWNLOAD/automake-1.13.4.tar.gz
# cd automake-1.13.4
# ./configure --prefix=$OMPI_TOOLS_PREFIX
# make
# make install
# 
# cd $OMPI_SOURCE
# tar -xvzf $OMPI_DOWNLOAD/libtool-2.4.2.tar.gz
# cd libtool-2.4.2
# ./configure --prefix=$OMPI_TOOLS_PREFIX
# make
# make install

cd $OMPI_SOURCE
## git clone https://github.com/open-mpi/ompi.git
cd ompi
git checkout master
git pull
git checkout $OMPI_COMMIT
./autogen.pl

export OMPI_BUILD=$OMPI_DIR/build/$OMPI_LABEL
mkdir -p $OMPI_BUILD
cd $OMPI_BUILD
export CFLAGS=-O3
export CXXFLAGS=-O3

echo "========================================="
echo "OMPI_DIR      : $OMPI_DIR"
echo "OMPI_SOURCE   : $OMPI_SOURCE"
echo "OMPI_BUILD    : $OMPI_BUILD"
echo "OMPI_INSTALLED: $OMPI_INSTALLED"
echo "OMPI_LABEL    : $OMPI_LABEL"
echo "modules       :"
module list 2>&1 | sort
echo "========================================="

# $OMPI_SOURCE/ompi/configure \
#     --enable-orterun-prefix-by-default \
#     --with-devel-headers \
#     --disable-debug \
#     --enable-static \
#     --disable-pmix-dstore \
#     --prefix=$OMPI_INSTALLED/$OMPI_LABEL
$OMPI_SOURCE/ompi/configure               \
    --prefix=$OMPI_INSTALLED/$OMPI_LABEL  \
    --enable-orterun-prefix-by-default    \
    --with-devel-headers                  \
    --disable-debug                       \
    --enable-static                       \
    --enable-heterogeneous                \
    --enable-timing                       \
    --enable-mpi-cxx                      \
    --enable-install-libpmix              \
    --enable-pmix-timing                  \
    --with-pmix=internal                  \
    --with-ugni                           \
    --with-pmi=/opt/cray/pmi/5.0.12/      \
    --with-tm                             \
    --with-alps=yes                       \

  # --with-cray-pmi                       \
make -j 32
make install

# install libffi on systems which don't have it, so that the pilot ve can
# install the `orte_cffi` python module.
# libffi documentation needs texi2html which is not commonly available, so we
# disable documentation.

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


cd $OMPI_SOURCE
## git clone https://github.com/libffi/libffi.git
cd libffi
git pull
./autogen.sh
./configure --prefix=$OMPI_TOOLS_PREFIX --disable-docs
make
make install


# we also install gromacs
cd $OMPI_SOURCE
rm -rf gromacs-5.1.4
tar xf $OMPI_DOWNLOAD/gromacs-5.1.4.tar.gz
cd gromacs-5.1.4

echo '--------------------------------------'
pwd -P
echo module use --append $OMPI_MODULE_BASE
echo module load openmpi/$OMPI_LABEL
echo '--------------------------------------'

module use --append $OMPI_MODULE_BASE
module load openmpi/$OMPI_LABEL
module list

set -x
cmake \
  -DCMAKE_C_COMPILER=mpicc \
  -DCMAKE_CXX_COMPILER=mpiCC \
  -DGMX_MPI=on \
  -DCMAKE_INSTALL_PREFIX=$OMPI_INSTALLED/$OMPI_LABEL \
  -DBUILD_SHARED_LIBS=ON \
  -DGMX_BUILD_OWN_FFTW=ON \
  -DGMX_OPENMP=OFF \
  -DGMX_SIMD=AVX_128_FMA
set +x
make
make install


# this should not be needed - but just in case someone sources this script,
# we try to end up where we started.
cd $orig


