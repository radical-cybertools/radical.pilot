
.. _chapter_installation:

************
Installation
************

Requirements 
============

RADICAL-Pilot needs Python >= 2.6. All dependencies are installed automatically 
by the installer. Besides that, RADICAL-Pilot needs access to a MongoDB 
database that is reachable from the internet. User groups within the same 
institution or project can share a single MongoDB instance. 

MongoDB is standard software and available in most Linux distributions. At the 
end of this section, we provide brief instructions how to set up a MongoDB 
server and discuss some advanced topics, like SSL support and authentication 
to increased the security of RADICAL-Pilot. 


Installation
============

To install RADICAL-Pilot in a virtual environment, open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/myenv
    source $HOME/myenv/bin/activate
    pip install radical.pilot

Next, you can do a quick sanity check to make sure that the the packages have
been installed properly. In the same virtualenv, run:

.. code-block:: bash

    radicalpilot-version

This should return the version of the RADICAL-Pilot installation, e.g., `0.X.Y`.

.. note::

  Note that some Python installations have a broken multiprocessing module -- if you
  experience the following error during installation::

    Traceback (most recent call last):
      File "/usr/lib/python2.6/atexit.py", line 24, in _run_exitfuncs
        func(*targs, **kargs)
      File "/usr/lib/python2.6/multiprocessing/util.py", line 284, in _exit_function
        info('process shutting down')
    TypeError: 'NoneType' object is not callable

  you may need to move to Python 2.7 (see http://bugs.python.org/issue15881).


Installation from Source
========================

If you are planning to contribute to the RADICAL-Pilot codebase, or if you want 
to use the latest and greatest development features, you can download
and install RADICAL-Pilot directly from the sources.

First, you need to check out the sources from GitHub.

.. code-block:: bash

    git@github.com:radical-cybertools/radical.pilot.git

Next, run the installer directly from the source directoy (assuming you have 
set up a vritualenv):

.. code-block:: bash
 
    python setup.py install


Installing MongoDB
==================


With SSL Support
----------------

The standard MongoDB distribution doesn't come with SSL-support enabled, so 
you have to build it from sources. On Ubuntu Linux, the following steps 
should do the trick:

.. code-block:: bash

    apt-get -y install scons libssl-dev libboost-filesystem-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev
    git clone -b r2.6.3 https://github.com/mongodb/mongo.git
    cd mongo
    scons --64 --ssl all
    scons --64 --ssl --prefix=/usr install
