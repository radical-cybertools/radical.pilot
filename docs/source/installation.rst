
.. _chapter_installation:

************
Installation
************

Requirements 
============

RADICAL-Pilot requires the following packages:

* Python >= 2.7
* virtualenv >= 1.11
* pip >= 1.5

If you plan to use RADICAL-Pilot on remote machines, you would also require to setup a 
password-less ssh login to the particular machine. ( `help <http://www.debian-administration.org/article/152/Password-less_logins_with_OpenSSH>`_ )


All dependencies are installed automatically by the installer. Besides that, RADICAL-Pilot needs 
access to a MongoDB database that is reachable from the internet. User groups within the same 
institution or project can share a single MongoDB instance. 

MongoDB is standard software and available in most Linux distributions. At the 
end of this section, we provide brief instructions how to set up a MongoDB 
server and discuss some advanced topics, like SSL support and authentication 
to increased the security of RADICAL-Pilot. 


Installation
============

To install RADICAL-Pilot in a virtual environment, open a terminal and run:

If your shell is BASH,

.. code-block:: bash

    virtualenv $HOME/myenv
    source $HOME/myenv/bin/activate
    pip install radical.pilot


If your shell is CSH,

.. code-block:: csh

    virtualenv $HOME/myenv
    source $HOME/myenv/bin/activate.csh
    pip install radical.pilot    

Next, you can do a quick sanity check to make sure that the the packages have
been installed properly. In the same virtualenv, run:

.. code-block:: bash

    radicalpilot-version


.. note::

  If your shell is CSH you also need to do,

  .. code-block:: csh

    rehash

  This will reset the PATH variable to also point to the packages which were just installed.


** Installation is complete !**


Preparing the Environment
=========================

ExTASY is developed using Ensemble MD Toolkit which is a client-side library and relies on a set of external software packages. One of these packages is `radical.pilot <http://radicalpilot.readthedocs.org>`_, an HPC cluster resource access and management library. It can access HPC clusters remotely via SSH and GSISSH, but it requires (a) a MongoDB server and (b) a properly set-up SSH environment.

.. figure:: ../images/hosts_and_ports.png
   :width: 360pt
   :align: center
   :alt: MongoDB and SSH ports.


MongoDB Server
--------------

The MongoDB server is used to store and retrieve operational data during the
execution of an application using RADICAL-Pilot. The MongoDB server must
be reachable on **port 27017** from **both**, the host that runs the
Ensemble MD Toolkit application and the host that executes the MD tasks, i.e.,
the HPC cluster (see blue arrows in the figure above). In our experience,
a small VM instance (e.g., Amazon AWS) works exceptionally well for this.

.. warning:: If you want to run your application on your laptop or private
             workstation, but run your MD tasks on a remote HPC cluster,
             installing MongoDB on your laptop or workstation won't work.
             Your laptop or workstations usually does not have a public IP
             address and is hidden behind a masked and firewalled home or office
             network. This means that the components running on the HPC cluster
             will not be able to access the MongoDB server.

A MongoDB server can support more than one user. In an environment where
multiple users use Ensemble MD Toolkit applications, a single MongoDB server
for all users / hosts is usually sufficient.

Install your own MongoDB
------------------------

Once you have identified a host that can serve as the new home for MongoDB,
installation is straight forward. You can either install the MongoDB
server package that is provided by most Linux distributions, or
follow the installation instructions on the MongoDB website:

http://docs.mongodb.org/manual/installation/

MongoDB-as-a-Service
--------------------

There are multiple commercial providers of hosted MongoDB services, some of them
offering free usage tiers. We have had some good experience with the following:

* https://mongolab.com/


Setup an easy method for SSH Access to machines
-----------------------------------------------

An easy way to setup SSH Access to multiple remote machines is to create a file ``~/.ssh/config``.
Suppose the url used to access a specific machine is ``foo@machine.example.com``. You can create an entry in this config file as follows:

.. parsed-literal::

    # contents of $HOME/.ssh/config
    Host mach1
        HostName machine.example.com
        User foo

Now you can login to the machine by ``ssh mach1``.


Source: http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/


Troubleshooting
===============

**Missing virtualenv**

This should return the version of the RADICAL-Pilot installation, e.g., `0.X.Y`.

If virtualenv **is not** installed on your system, you can try the following.

.. code-block:: bash

    wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
    tar xzf virtualenv-1.9.tar.gz

    python virtualenv-1.9/virtualenv.py $HOME/myenv
    source $HOME/myenv/bin/activate

**TypeError: 'NoneType' object is not callable**

Note that some Python installations have a broken multiprocessing module -- if you
experience the following error during installation::

  Traceback (most recent call last):
    File "/usr/lib/python2.7/atexit.py", line 24, in _run_exitfuncs
      func(*targs, **kargs)
    File "/usr/lib/python2.7/multiprocessing/util.py", line 284, in _exit_function
      info('process shutting down')
  TypeError: 'NoneType' object is not callable

  you may need to move to Python 2.7 (see http://bugs.python.org/issue15881).


