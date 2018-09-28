
.. _chapter_installation:

************
Installation
************

Requirements 
============

RADICAL-Pilot requires the following packages:

* Python >= 2.7 (including development headers)
* virtualenv >= 1.11
* pip == 1.4.1

or
* Anaconda Python 2.7

If you plan to use RADICAL-Pilot on remote machines, you would also require to
setup a password-less ssh login to the particular machine. 
(`help <http://www.debian-administration.org/article/152/Password-less_logins_with_OpenSSH>`_)


All dependencies are installed automatically by the installer. Besides that,
RADICAL-Pilot needs access to a MongoDB database that is reachable from the
internet. User groups within the same institution or project usually share
a single MongoDB instance.  MongoDB is standard software and available in most
Linux distributions. At the end of this section, we provide brief instructions
how to set up a MongoDB server and discuss some advanced topics, like SSL
support and authentication to increased the security of RADICAL-Pilot. 


Installation
============

RADICAL-Pilot is distributed via PyPi and Conda-Forge. To install RADICAL-Pilot
to a virtual environment do:

via PyPi
-----------------
.. code-block:: bash

    virtualenv --system-site-packages $HOME/ve
    source $HOME/ve/bin/activate
    pip install radical.pilot

via Conda-Forge
-----------------
.. code-block:: bash

    conda create -n ve -y python=2.7
    source activate ve
    conda install radical.pilot -c conda-forge

For a quick sanity check, to make sure that the the packages have been installed
properly, run:

.. code-block:: bash

    $ radicalpilot-version
    0.50.8

The exact output will obviously depend on the exact version of RP which got
installed.


** Installation is complete !**


Preparing the Environment
=========================

MongoDB Service
---------------

RP requires access to a MongoDB server.  The MongoDB server is used to store and
retrieve operational data during the execution of an application using
RADICAL-Pilot. The MongoDB server must be reachable from **both**, the host that
runs the RP application and the target resource which runs the pilots.  

.. warning:: If you want to run your application on your laptop or private
             workstation, but run your MD tasks on a remote HPC cluster,
             installing MongoDB on your laptop or workstation won't work.
             Your laptop or workstations usually does not have a public IP
             address and is hidden behind a masked and firewalled home or office
             network. This means that the components running on the HPC cluster
             will not be able to access the MongoDB server.

Any MongoDB installation should work out, as long as RP is allowed to create
databases and collections (which is the default user setting in MongoDB).

The MongoDB location is communicated to RP via the environment variable
``RADICAL_PILOT_DBURL``.  The value will have the form

.. code-block:: bash

    export RADICAL_PILOT_DBURL="mongodb://user:pass@host:port/dbname"
    export RADICAL_PILOT_DBURL="mongodb://host:port/dbname"

Many MongoDB instances are by default unsecured, and thus do not require the
``user:pass@`` part of the URL.  For production runs, and for runs on large
secured resources, we strongly recommend the usage of a secured MongoDB
instance!

The ``dbname`` component in the database url can be any valid MongoDB database
name (ie. it musy not contain dots).RP will not create that DB on the fly and 
requires the DB to be setup prior to creating the session object. But RP will 
create collections in that DB on its own, named after RP session IDs.


A MongoDB server can support more than one user. In an environment where
multiple users use RP applications, a single MongoDB server for all users
/ hosts is usually sufficient.  We recommend the use of separate databases per
user though, so please set the ``dbname`` to something like ``db_joe_doe``.


**Install your own MongoDB**

Once you have identified a host that can serve as the new home for MongoDB,
installation is straight forward. You can either install the MongoDB
server package that is provided by most Linux distributions, or
follow the installation instructions on the MongoDB website:

* http://docs.mongodb.org/manual/installation/


**MongoDB-as-a-Service**

There are multiple commercial providers of hosted MongoDB services, some of them
offering free usage tiers. We have had some good experience with the following:

* https://mongolab.com/


Setup SSH Access to Target Resources
------------------------------------

An easy way to setup SSH Access to multiple remote machines is to create a file
``~/.ssh/config``.  Suppose the url used to access a specific machine is
``foo@machine.example.com``. You can create an entry in this config file as
follows:

.. code::

    # contents of $HOME/.ssh/config
    Host mach1
        HostName machine.example.com
        User foo

Now you can login to the machine by using ``ssh mach1``.  Please make also sure
that your ssh keys are registered on the target resources -- while RP can in
principle handle password based login, the repeated prompts for passwords makes
RP applications very difficult to use.

Source: http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/


Troubleshooting
===============

**Missing virtualenv**

This should return the version of the RADICAL-Pilot installation, e.g., ``0.X.Y``.

If virtualenv **is not** installed on your system, you can try the following.

.. code-block:: bash

    wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
    tar xzf virtualenv-1.9.tar.gz

    python virtualenv-1.9/virtualenv.py $HOME/ve
    source $HOME/ve/bin/activate


**Installation Problems**

Many installation problems boil down to one of two causes:  an Anaconda based Python
distribution, or an incompatible version of pip/setuptools.

Many recent systems, specifically in the academic community, install Python in
its incarnation as Anaconda Distribution.  RP is not yet able to function in
that environment.  While support of Anaconda is planned in the near future, you
will have to revert to a 'normal' Python distribution to use RP.

Python supports a large variety of module deployment paths: ``easy_install``,
``setuptools`` and ``pip`` being the most prominent ones for non-compilable
modules.  RP only supports ``pip``, and even for pip we do not attempt to keep
up with its vivid evolution.  If you encounter pip errors, please downgrade pip
to version ``1.4.1``, via

.. code-block:: bash

    $ pip install --upgrade pip==1.4.1

If you continue to encounter problems, please also fix the version of setuptools
to ``0.6c11`` via

.. code-block:: bash

    $ pip install --upgrade setuptools==0.6c11

.. note::

    RADICAL-Pilot can be installed under Anaconda, although that mode is not
    tested as thoroughly compared to installation under non-Anaconda Python.


**Mailing Lists**

If you encounter any errors, please do not hesitate to contact us via the
mailing list:

* https://groups.google.com/d/forum/radical-pilot-users

We also appreciate issues and bug reports via our public github tracker:

* https://github.com/radical-cybertools/radical.pilot/issues


