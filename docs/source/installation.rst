
.. _chapter_installation:

************
Installation
************

Requirements 
============

* Python >= 2.5

Install from GitHub
===================

Currently we only support installation via pip directly from GitHub. For the
future we are planning releases on PyPi as well.

To install SAGA-Pilot from the stable (main) branch in a virtual environment, do this:

.. code-block:: bash

    virtualenv $HOME/spenv
    source $HOME/spenv/bin/activate
    pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot

