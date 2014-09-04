
.. _chapter_faq:

**************************
Frequently Asked Questions
**************************

Q: I see the error "OperationFailure: too many namespaces/collections"
----------------------------------------------------------------------

.. code-block:: bash

    Traceback (most recent call last):
      File "/tmp/test/bin/extasy", line 9, in <module>
        load_entry_point('radical.ensemblemd.extasy==0.1', 'console_scripts', 'extasy')()
      File "/tmp/test/lib/python2.7/site-packages/radical.ensemblemd.extasy-0.1-py2.7.egg/radical/ensemblemd/extasy/bin/runme.py", line 82, in main
        umgr,session=startPilot()
      File "/tmp/test/lib/python2.7/site-packages/radical.ensemblemd.extasy-0.1-py2.7.egg/radical/ensemblemd/extasy/bin/runme.py", line 45, in startPilot
        pmgr = radical.pilot.PilotManager(session=session)
      File "/tmp/test/lib/python2.7/site-packages/radical.pilot-0.18.RC2-py2.7.egg/radical/pilot/pilot_manager.py", line 120, in __init__
        db_connection_info=session._connection_info)
      File "/tmp/test/lib/python2.7/site-packages/radical.pilot-0.18.RC2-py2.7.egg/radical/pilot/controller/pilot_manager_controller.py", line 88, in __init__
        pilot_launcher_workers=pilot_launcher_workers
      File "/tmp/test/lib/python2.7/site-packages/radical.pilot-0.18.RC2-py2.7.egg/radical/pilot/db/database.py", line 253, in insert_pilot_manager
        result = self._pm.insert(pilot_manager_json)
      File "build/bdist.linux-x86_64/egg/pymongo/collection.py", line 412, in insert
      File "build/bdist.linux-x86_64/egg/pymongo/mongo_client.py", line 1121, in _send_message
      File "build/bdist.linux-x86_64/egg/pymongo/mongo_client.py", line 1063, in __check_response_to_last_error
    pymongo.errors.OperationFailure: too many namespaces/collections


A: Try Cleaning-up MongoDB.
---------------------------

This can happen rarely if radical.pilot keeps on failing in an unexpected way 
and without clean up the database upon termination. Currenlty the only 
workaround for this is to manually clean-up the database using the ``radicalpilot-clean``
and ``radicalpilot-cleanup`` tools (both are installed with radical.pilot). 
The former lists and purges database entries older than a specified age; the 
atter closes an existing session (and thus also cleanes its database entry). 


Q: I see the error "Host key verification failed" in AGENT.STDERR.
------------------------------------------------------------------

The AGENT.STDERR file shows the following error and the pilot never starts
running:

.. code-block:: bash

    Host key verification failed.g
    kill: 19932: No such process

A: Set up password-less, intra-node SSH access.
-----------------------------------------------

Even though this should already be set up by default on many HPC clusters, it
is not always the case. The following instructions will help you to set up
password-less SSH acces:

.. code-block:: bash

    cd ~/.ssh/
    ssh-keygen -t rsa

For passphrase just hit return. The result should look something like this:

.. code-block:: bash

    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/e290/e290/oweidner/.ssh/id_rsa): 
    Enter passphrase (empty for no passphrase): 
    Enter same passphrase again: 
    Your identification has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.
    Your public key has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.pub.
    The key fingerprint is:
    73:b9:cf:45:3d:b6:a7:22:72:90:28:0a:2f:8a:86:fd oweidner@eslogin001
    The key's randomart image is:
    +--[ RSA 2048]----+
    |                 |
    |                 |
    |                 |
    |           .   . |
    |       .S.o   .o.|
    |.   . . oo . .. o|
    |.+ . .   ..   ...|
    |+.+     . oo.. ..|
    |=. .E    o .o..  |
    +-----------------+

Next, add you newly generated key to ~/.ssh/authorized_keys:

.. code-block:: bash

    cat id_rsa.pub >> ~/.ssh/authorized_keys