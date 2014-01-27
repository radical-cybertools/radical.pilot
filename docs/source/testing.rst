
.. _chapter_machconf:

*******
Testing
*******

Introduction
============

Along with SAGA-Pilot's functionality, we develop a growing set of unit 
tests. The unit test source code can be found in ``sinon/tests``. You 
can run the unit tests directly from the source directory without haing
to install SAGA-Pilot first:

.. code-block:: bash

    SINON_VERBOSE=debug \
    python setup.py test

.. note:: 

    If you run the same command in an environment where SAGA-Pilot is already
    installed, the unit tests will test the installed version instead of the 
    source version. 

Remote Testing 
==============

The SAGA-Pilot unit tests use pilot agents launched on the local machine
(`localhost`) by default. However, it is possible to run a subset of the  unit
tests (``sinon/tests/remote/``) on a remote machine. Remote testing can  be
controlled via a set of environment variables:

+------------------------------------+---------------------------------------------------------------+
| Environment Variable               | What                                                          |
+====================================+===============================================================+
| ``SINON_TEST_REMOTE_RESOURCE``     | The name (key) of the resource.                               | 
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_REMOTE_SSH_USER_ID``  | The user ID on the remote system.                             |
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_REMOTE_SSH_USER_KEY`` | The SSH key to use for the connection.                        |
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_REMOTE_WORKDIR``      | The working directory on the remote system.                   |
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_REMOTE_CORES``        | The number of cores to allocate.                              |
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_REMOTE_NUM_CUS``      | The number of Compute Units to run.                           |
+------------------------------------+---------------------------------------------------------------+
| ``SINON_TEST_TIMEOUT``             | Set a timeout in minutes after which the tests will terminate.|
+------------------------------------+---------------------------------------------------------------+


So if for example you want to run the unit tests on Futuregrid's _India_ cluster 
(http://manual.futuregrid.org/hardware.html), run

.. code-block:: bash

    SINON_VERBOSE=debug \
    SINON_TEST_REMOTE_SSH_USER_ID=oweidner # optional \
    SINON_TEST_REMOTE_RESOURCE=futuregrid.INDIA \
    SINON_TEST_REMOTE_WORKDIR=/N/u/oweidner/sinon \
    SINON_TEST_REMOTE_CORES=32 \
    SINON_TEST_REMOTE_NUM_CUS=64 \
    python setup.py test

.. note:: 
 
    Be aware that it can take quite some time for pilots to get scheduled on 
    the remote system. You can set ``SINON_TEST_TIMEOUT`` to force the tests 
    to abort after a given number of minutes.
