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

export OMPI_DIR=$HOME/radical/ompi/                  # target location for install
export OMPI_DIR=/lustre/atlas2/csc230/world-shared/openmpi/
export OMPI_COMMIT=a3ac67be0d
export OMPI_COMMIT=539f71d                           # OpenMPI commit to install
export OMPI_LABEL=$(date '+%Y_%m_%d'_${OMPI_COMMIT}) # module flag for installed version
export MAKEFLAGS=-j16                                # speed up build on multicore machines


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
# and OMPI_ALL_MODULES=/projects/sciteam/gk4/openmpi/modules.

export OMPI_ALL_MODULES=$OMPI_DIR/modules
export OMPI_MODULE=$OMPI_ALL_MODULES/openmpi
export OMPI_INSTALLED=$OMPI_DIR/installed

mkdir -p $OMPI_DOWNLOAD
mkdir -p $OMPI_SOURCE

cd $OMPI_DOWNLOAD
wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
wget http://ftp.gnu.org/gnu/automake/automake-1.13.4.tar.gz
wget http://ftp.gnu.org/gnu/libtool/libtool-2.4.2.tar.gz
wget http://ftp.gnu.org/gnu/m4/m4-1.4.16.tar.gz

cd $OMPI_SOURCE
tar -xvzf $OMPI_DOWNLOAD/m4-1.4.16.tar.gz
cd m4-1.4.16
./configure --prefix=$OMPI_TOOLS_PREFIX
make
make install

cd $OMPI_SOURCE
tar -xvzf $OMPI_DOWNLOAD/autoconf-2.69.tar.gz
cd autoconf-2.69
./configure --prefix=$OMPI_TOOLS_PREFIX
make
make install

cd $OMPI_SOURCE
tar -xvzf $OMPI_DOWNLOAD/automake-1.13.4.tar.gz
cd automake-1.13.4
./configure --prefix=$OMPI_TOOLS_PREFIX
make
make install

cd $OMPI_SOURCE
tar -xvzf $OMPI_DOWNLOAD/libtool-2.4.2.tar.gz
cd libtool-2.4.2
./bootstrap
./configure --prefix=$OMPI_TOOLS_PREFIX
make
make install

cd $OMPI_SOURCE
git clone https://github.com/open-mpi/ompi.git
cd ompi
git checkout $OMPI_COMMIT
./autogen.pl

export OMPI_BUILD=$OMPI_DIR/build/$OMPI_LABEL
mkdir -p $OMPI_BUILD
cd $OMPI_BUILD
export CFLAGS=-O3
export CXXFLAGS=-O3
$OMPI_SOURCE/ompi/configure \
    --enable-orterun-prefix-by-default \
    --with-devel-headers \
    --disable-debug \
    --enable-static \
    --disable-pmix-dstore \
    --prefix=$OMPI_INSTALLED/$OMPI_LABEL
make
make install


# install libffi on systems which don't have it, so that the pilot ve can
# install the `orte_cffi` python module.
# libffi documentation needs texi2html which is not commonly available, so we
# disable documentation.
#
# cd $OMPI_SOURCE
# git clone https://github.com/libffi/libffi.git
# cd libffi
# ./autogen.sh
# ./configure --prefix=$OMPI_TOOLS_PREFIX --disable-docs
# make
# make install



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


# this should not be needed - but just in case someone sources this script,
# we try to end up where we started.
cd $orig

