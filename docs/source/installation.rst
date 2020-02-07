
.. _chapter_installation:

************
Installation
************

RADICAL-Pilot is a Python module which can be installed either via the python
package installed ``pip`` or as part of the ``Anaconda`` distribution.
RADICAL-Pilot **must** be installed in a virtual environment and **cannot** be
installed as a system-wide Python package.

Requirements 
============

RADICAL-Pilot requires: software packages, a properly configured ``ssh``
login, and a ``MongoDB`` server.

Software
--------

RADICAL-Pilot requires the following packages:

* Python >= 3.7
* virtualenv >= 16
* pip >= 20

or

* Anaconda Python >= 3.7

All other dependencies are installed automatically by RADICAL-Pilot installer.


SSH
---

.. If you plan to use RADICAL-Pilot on remote machines, you would also require to
.. setup a password-less ssh login to the particular machine. 
.. (`help <http://www.debian-administration.org/article/152/Password-less_logins_with_OpenSSH>`_)

RADICAL-Pilot requires to setup a password-less ``ssh`` login between the
machine where your application is executed and the machine where the tasks of
your application will be executed. For example, if you execute your
application written with the RADICAL-Pilot API on a virtual machine hosted on a
public or private cloud provider, and you want to execute your application's
tasks on an XSEDE HPC machine, you will have to configure a password-less
``(gsi)ssh`` login between the virtual and the HPC machine. (Here one of
the many guides available about `How to Setup Passwordless SSH Login
<https://linuxize.com/post/how-to-setup-passwordless-ssh-login/>`_).


MongoDB
-------

.. RADICAL-Pilot needs access to a ``MongoDB`` database that is reachable from
.. the Internet. User groups within the same institution or project usually share
.. a single MongoDB instance.  MongoDB is standard software and available in most
.. Linux distributions. 

.. At the end of this section, we provide brief instructions how to set up a
.. MongoDB server and discuss some advanced topics, like SSL support and
.. authentication to increased the security of RADICAL-Pilot.

The MongoDB server is used to store and retrieve operational data during the
execution of an application which uses RADICAL-Pilot. The MongoDB server must
be reachable from **both**, the host that runs the RADICAL-Pilot application
and the target resource which runs the pilots.

.. warning:: If you want to run your application on a workstation that does 
             **not** have a public IP address, but run your tasks on a remote 
             HPC cluster, installing MongoDB on your workstation won't work. 
             Without a public IP and a port publicly reachable, the components 
             of RADICAL-Pilot running on the HPC cluster will not be able to 
             access the MongoDB server.

.. Any MongoDB installation should work out, as long as RADICAL-Pilot is
.. allowed to create collections on a previously created database (DB), which
.. is the default user setting in MongoDB.

The MongoDB location is communicated to RADICAL-Pilot via the environment
variable ``RADICAL_PILOT_DBURL``.  The value will have the form:

.. code-block:: bash

    export RADICAL_PILOT_DBURL="mongodb://user:pass@host:port/dbname"
.. export RADICAL_PILOT_DBURL="mongodb://host:port/dbname"

Many MongoDB instances are by default unsecured: we **strongly** recommend the
usage of a secured MongoDB instance!

The ``dbname`` component in the database URL can be any valid MongoDB database
name (i.e., it must not contain dots). **RADICAL-Pilot will not create that
DB** on the fly and requires the DB to be setup prior to creating the session
object. But RADICAL-Pilot will create collections in that DB on its own, named
after RADICAL-Pilot session IDs.

.. A MongoDB server can support more than one user. In an environment where
.. multiple users use RADICAL-Pilot applications, a single MongoDB server for all
.. users is usually sufficient.  We recommend the use of separate databases per
.. user though, so please set the ``dbname`` to something like ``db_joe_doe``.

Once you have identified a host that can serve as the new home for MongoDB,
installation is straight forward. You can either install the MongoDB server
package that is provided by most Linux distributions, or follow the
installation instructions on the `MongoDB website
<http://docs.mongodb.org/manual/installation/>`_.

If you don't have access to a suitable machine with a public IP, you can open
a ticket with RADICAL lab and we will provide a testing MongoDB instance for
you.

Installation
============

RADICAL-Pilot is distributed via PyPi and Conda-Forge. To install RADICAL-Pilot
to a virtual environment do:

via PyPi
--------
.. code-block:: bash

    virtualenv --system-site-packages $HOME/ve
    source $HOME/ve/bin/activate
    pip install radical.pilot

.. python3 -m venv $HOME/ve

via Conda-Forge
---------------
.. code-block:: bash

    conda create -n ve -y python=3.7
    conda activate ve
    conda install radical.pilot -c conda-forge

For a quick sanity check, to make sure that the packages have been installed
properly, run:

.. code-block:: bash

    $ radical-pilot-version
    1.0.2

The exact output will obviously depend on the exact version of RADICAL-Pilot which got
installed.


**Installation is complete!**


.. Preparing the Environment
.. =========================

.. MongoDB Service
.. ---------------

.. RADICAL-Pilot requires access to a MongoDB server.  


.. **Install your own MongoDB**


.. **MongoDB-as-a-Service**

.. There are multiple commercial providers of hosted MongoDB services, some of them
.. offering free usage tiers. We have had some good experience with the following:

.. * https://mongolab.com/


.. Setup SSH Access to Target Resources
.. ------------------------------------

.. An easy way to setup SSH Access to multiple remote machines is to create a
.. file ``~/.ssh/config``.  Suppose the url used to access a specific machine
.. is ``foo@machine.example.com``. You can create an entry in this config file
.. as follows:

.. code: :

..     # contents of $HOME/.ssh/config
..     Host mach1
..         HostName machine.example.com
..         User foo

.. Now you can login to the machine by using ``ssh mach1``.  Please make also
.. sure that your ssh keys are registered on the target resources -- while
.. RADICAL-Pilot can in principle handle password based login, the repeated
.. prompts for passwords makes RADICAL-Pilot applications very difficult to
.. use. To learn more about accessing remote machine using RADICAL-Pilot, see
.. the chapter `Using Local and Remote HPC Resources <./machconf.rst>`.

.. Source: http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/


.. Troubleshooting
.. ===============

.. Here a collection of common problems with installing RADICAL-Pilot. Please
.. open a (`support ticket
.. <https://github.com/radical-cybertools/radical.pilot/issues>`_) with RADICAL
.. Lab if your issue is not addressed by the following.


.. Missing virtualenv
.. ------------------

.. This should return the version of the RADICAL-Pilot installation, e.g., ``0.X.Y``.

.. If virtualenv **is not** installed on your system, you can try the following.

.. .. code-block:: bash

..     wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
..     tar xzf virtualenv-1.9.tar.gz

..     python virtualenv-1.9/virtualenv.py $HOME/ve
..     source $HOME/ve/bin/activate


.. Incompatibilities
.. -----------------

.. Many installation problems depends on an incompatible version of
.. ``pip`` and ``setuptools``.

.. Many recent systems, specifically in the academic community, install Python in
.. its incarnation as Anaconda Distribution.  RADICAL-Pilot is not yet able to function in
.. that environment.  While support of Anaconda is planned in the near future, you
.. will have to revert to a 'normal' Python distribution to use RADICAL-Pilot.

.. Python supports a large variety of module deployment paths: ``easy_install``,
.. ``setuptools`` and ``pip`` being the most prominent ones for non-compilable
.. modules.  RADICAL-Pilot only supports ``pip``, and even for pip we do not attempt to keep
.. up with its vivid evolution.  If you encounter pip errors, please downgrade pip
.. to version ``1.4.1``, via:

.. .. code-block:: bash

..     $ pip install --upgrade pip==1.4.1

.. If you continue to encounter problems, please also fix the version of setuptools
.. to ``0.6c11`` via

.. .. code-block:: bash

..     $ pip install --upgrade setuptools==0.6c11

.. .. note::

..     RADICAL-Pilot can be installed under Anaconda, although that mode is not
..     tested as thoroughly compared to installation under non-Anaconda Python.


Support
=======

RADICAL-Pilot undergoes constant evolution, implementing new capabilities,
supporting new resources and keeping up with the progressing of its
dependencies. If you encounter any error, please do not hesitate to contact the
RADICAL lab team by opening an 
`issue <https://github.com/radical-cybertools/radical.pilot/issues>`_.

.. via the
.. mailing list:

.. * https://groups.google.com/d/forum/radical-pilot-users

.. We also appreciate issues and bug reports via our public github tracker:

.. * https://github.com/radical-cybertools/radical.pilot/issues


