
.. _chapter_installation:

************
Installation
************

Requirements 
============

SAGA-Pilot relies on a set of external software packages, all of which get 
installed automatically as dependencies. 


* Python >= 2.5

* setuptools (https://pypi.python.org/pypi/setuptools)
* saga-python (https://pypi.python.org/pypi/saga-python)
* radical.utils (https://pypi.python.org/pypi/radical.utils/)
* python-hostlist (https://pypi.python.org/pypi/python-hostlist)

Installation
============

Currently SAGA-Pilot can only be installed via pip directly from GitHub. 

To install SAGA-Pilot from the stable (main) branch in a virtual environment, 
open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/myenv
    source $HOME/myenv/bin/activate
    pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot

Next, you can do a quick sanity check to make sure that the the packages have
been installed properly. In the same virtualenv, run:

.. code-block:: bash

    sagapilot-version

This should return the version of the SAGA-Pilot installation, e.g., `0.X.Y`.

Installation from Source
========================

If you are planning to contribute to the SAGA-Pilot codebase, you can download
and install SAGA-Python directly from the sources.

First, you need to check out the sources from GitHub.

.. code-block:: bash

    git@github.com:saga-project/saga-pilot.git

Next, run the installer directly from the source directoy (assuming you have 
set up a vritualenv):

.. code-block:: bash
 
    python setup.py install

Optionally, you can try to run the unit tests:

.. code-block:: bash

    python setup.py test
