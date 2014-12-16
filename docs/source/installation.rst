
.. _chapter_installation:

************
Installation
************

Requirements 
============

RADICAL-Pilot needs Python >= 2.7. All dependencies are installed automatically
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
