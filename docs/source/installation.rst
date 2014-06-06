
.. _chapter_installation:

************
Installation
************

Requirements 
============

RADICAL-Pilot relies on a set of external software packages, all of which get 
installed automatically as dependencies. 


* Python >= 2.6
    

* setuptools (https://pypi.python.org/pypi/setuptools)
* saga-python (https://pypi.python.org/pypi/saga-python)
* radical.utils (https://pypi.python.org/pypi/radical.utils/)
* python-hostlist (https://pypi.python.org/pypi/python-hostlist)
* pymongo (https://pypi.python.org/pypi/pymongo/)

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

Optionally, you can try to run the unit tests:

.. code-block:: bash

    python setup.py test

.. note:: More on testing can be found in chapter :ref:`chapter_testing`.
