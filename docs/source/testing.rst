
.. _chapter_testing:

*******
Testing
*******

Introduction
============

Along with RADICAL-Pilot functionalities, we are developing a growing set of
unit tests. The source code of the unit tests can be found in
``src/radical/pilot/tests``. You can run the unit tests via `pytest`:

.. code-block:: bash

    export RADICAL_PILOT_LOG_LVL=debug
    pytest tests/


Remote Testing
==============

.. warning::

   Remote Testing is disabled in the current release!


By default, the unit tests of RADICAL-Pilot use pilot agents launched on the
local machine (`localhost`). However, it is possible to run a subset of the
unit tests (``src/radical/pilot/tests/remote/``) on a remote machine. Remote
testing can  be controlled via a set of environment variables:

+-------------------------------------------+-------------------------------------+
| Environment Variable                      | What                                |
+===========================================+=====================================+
| ``RADICAL_PILOT_TEST_REMOTE_RESOURCE``    | Name (key) of the resource.         |
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_REMOTE_SSH_USER_ID`` | User ID on the remote system.       |
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_REMOTE_SSH_USER_KEY``| SSH key to use for the connection.  |
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_REMOTE_WORKDIR``     | Work directory on the remote system.|
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_REMOTE_CORES``       | Number of cores to allocate.        |
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_REMOTE_NUM_CUS``     | Number of Compute Units to run.     |
+-------------------------------------------+-------------------------------------+
| ``RADICAL_PILOT_TEST_TIMEOUT``            | Test timeout in minutes.            |
+-------------------------------------------+-------------------------------------+


For example, if you want to run the unit tests on the XSEDE _Bridges_ cluster
(https://portal.xsede.org/psc-bridges), run

.. code-block:: bash

    RADICAL_PILOT_LOG_LVl=DEBUG \
    RADICAL_PILOT_TEST_REMOTE_SSH_USER_ID=<your_user_id> # optional \
    RADICAL_PILOT_TEST_REMOTE_RESOURCE=xsede.bridges \
    RADICAL_PILOT_TEST_REMOTE_WORKDIR=<absolute_path> \
    RADICAL_PILOT_TEST_REMOTE_CORES=16 \
    RADICAL_PILOT_TEST_REMOTE_NUM_CUS=64 \
    python setup.py test

.. note::

    Be aware that it can take quite some time for pilots to get scheduled on
    the remote system. You can set ``RADICAL_PILOT_TEST_TIMEOUT`` to force the tests
    to abort after a given number of minutes.


Adding New Tests
================

If you want to add a new tests, for example to reproduce an error that you have
encountered, please follow this procedure:

In the ``tests/issues/`` directory, create a new file. If applicable, name it
after the issues number in the RADICAL-Pilot 
`issues tracker <https://github.com/radical-cybertools/radical.pilot/issues>`_, 
e.g., ``issue_123.py``.

The content of the file should look like this (make sure to change the class
name):

.. code-block:: python

    import sys
    import radical.pilot

    #-----------------------------------------------------------------------------
    #
    class TestIssue123(object):

        #-------------------------------------------------------------------------
        #
        def test_issue_123_part_1(self):
            """ https://github.com/radical-cybertools/radical.pilot/issues/123
            """
            session = radical.pilot.Session()

            # Your test implementation

            session.close()

Now you can re-install RADICAL-Pilot and run your new test. In the source root,
run:

.. code-block:: python

    pip install --upgrade .
    pytest -v tests/issues/issue_123::TestIssue123
