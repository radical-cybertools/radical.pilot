
.. _chapter_machconf:

*******
Testing
*******

Introduction
============

In order to keep SAGA-Pilot applications free from clutter and 
machine-specific parameters and constants, SAGA-Pilot uses 
resource configration files.


Remote Testing 
==============

The SAGA-Pilot unit tests use pilot agents launched on the local machine 
(`localhost`) by default. However, it is possible to run the unit tests 
on a remote machine. Remote testing can be controlled via a set of 
environment variables:

+------------------------------------+---------------------------------------------+
| Environment Variable               | What                                        |
+====================================+=============================================+
| ``SINON_TEST_REMOTE_RESOURCE``     | The name (key) of the resource.             | 
+------------------------------------+---------------------------------------------+
| ``SINON_TEST_REMOTE_SSH_USER_ID``  | The user ID on the remote system.           |
+------------------------------------+---------------------------------------------+
| ``SINON_TEST_REMOTE_SSH_USER_KEY`` | The SSH key to use for the connection.      |
+------------------------------------+---------------------------------------------+
| ``SINON_TEST_REMOTE_WORKDIR``      | The working directory on the remote system. |
+------------------------------------+---------------------------------------------+
| ``SINON_TEST_REMOTE_CORES``        | The number of cores to allocate.            |
+------------------------------------+---------------------------------------------+
| ``SINON_TEST_REMOTE_NUM_CUS``      | The number of Compute Units to run.         |
+------------------------------------+---------------------------------------------+

So if for example you want to run the unit tests on Futuregrid's _India_ cluster 
(http://manual.futuregrid.org/hardware.html), run

.. code-block:: bash

    SINON_VERBOSE=info \
    SINON_TEST_REMOTE_RESOURCE= \
    python setup.py test

.. warning:: 
 
    Be aware that it can take quite some time for pilots to get scheduled on 
    the remote system. Since the unit tests launch at least O(10) pilots,
    executing the unit tests on a remote machine might take a significant 
    amount of time.
