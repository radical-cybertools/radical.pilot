
.. _chapter_faq:

**************************
Frequently Asked Questions
**************************

Here are some answers to frequently-asked questions. Got a question that isn't
answered here? Open an issue in the RADICAL-Pilot github 
`issue tracker <https://github.com/radical-cybertools/radical.pilot/issues>`_.

.. .. _mailing list: radical-pilot-users@googlegroups.com
.. .. _bug tracker: http://www.github.com/radical-cybertools/radical.pilot/issues/new

.. contents::
    :local:
    :depth: 2


Error "OperationFailure: too many namespaces/collections"
---------------------------------------------------------

.. code-block:: bash

    Traceback (most recent call last):
      File "application.py", line 120, in __init__
        db_connection_info=session._connection_info)
      File "/lib/python3.6/site-packages/radical/pilot/controller/pilot_manager_controller.py", line 88, in __init__
        pilot_launcher_workers=pilot_launcher_workers
      File "/lib/python3.6/site-packages/radical/pilot/db/database.py", line 253, in insert_pilot_manager
        result = self._pm.insert(pilot_manager_json)
      File "build/bdist.linux-x86_64/egg/pymongo/collection.py", line 412, in insert
      File "build/bdist.linux-x86_64/egg/pymongo/mongo_client.py", line 1121, in _send_message
      File "build/bdist.linux-x86_64/egg/pymongo/mongo_client.py", line 1063, in __check_response_to_last_error
    pymongo.errors.OperationFailure: too many namespaces/collections


This can happen if too many RADICAL-Pilot sessions are piling up in the
back-end database.  Database entries are removed when a RADICAL-Pilot session
is closed via ``session.close(cleanup=True)``.  However, if the application
fails and is not able to close the session, or if the session entry remains
purposefully in place for later analysis with ``radicalpilot-stats``, then
those entries add up over time.

RADICAL-Pilot provides two utilities which can be used to address this
problem: ``radicalpilot-close-session`` can be used to close a session when it
is not used anymore; ``radicalpilot-cleanup`` can be used to clean up all
sessions older than a specified number of hours or days, to purge orphaned
session entries in a bulk.


Error "Permission denied (publickey,keyboard-interactive)." in AGENT.STDERR or STDERR
-------------------------------------------------------------------------------------

The AGENT.STDERR file or the STDERR file in the unit directory shows the
following error and the pilot or unit never starts running:

.. code-block:: bash

    Permission denied (publickey,keyboard-interactive).
    kill: 19932: No such process


Even though this should already be set up by default on many HPC clusters, it
is not always the case. The following instructions will help you to set up
password-less SSH between the cluster nodes correctly.

Log-in to the **head-node** or **login-node** of the HPC cluster and run the 
following commands:  

.. code-block:: bash

    cd ~/.ssh/
    ssh-keygen -t rsa

**Do not enter a passphrase**. The result should look something like this:

.. code-block:: bash

    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/e290/e290/oweidner/.ssh/id_rsa): 
    Enter passphrase (empty for no passphrase): 
    Enter same passphrase again: 
    Your identification has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.
    Your public key has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.pub.
    The key fingerprint is:
    73:b9:cf:45:3d:b6:a7:22:72:90:28:0a:2f:8a:86:fd oweidner@eslogin001

Next, add you newly generated key to ~/.ssh/authorized_keys:

.. code-block:: bash

    cat id_rsa.pub >> ~/.ssh/authorized_keys

This should be all. Next time you run radical.pilot, you shouldn't see that 
error message anymore. 

(For more general information on SSH keys, check out this 
link: https://linuxize.com/post/how-to-setup-passwordless-ssh-login/)


Error "Failed to execvp() 'mybinary': No such file or directory (2)"
--------------------------------------------------------------------

The full error in STDERR is something like:

.. code-block:: bash

    [gcn-X-X.sdsc.edu:mpispawn_0][spawn_processes] Failed to execvp() 'mybinary': No such file or directory (2)


You need to specify the full path of the executable as mpirun_rsh is not able
to find it in the path.



Errors from setuptools when trying to use a virtualenv
------------------------------------------------------

This happens most likely because an upgrade of pip or setuptools failed.

We have seen occurrences where an update of setuptools or pip can make a
virtualenv unusable.  We don't have any suggestion on how to get the affected
virtualenv clean again. The easiest approach seems to just start over with a
new virtualenv. If the problem persists, try to use the default version of
setuptools and pip, i.e., do not upgrade them.


Error "Received message too long 1903391841"
--------------------------------------------

This error may show up in the DEBUG level logs during file staging or pilot
startup, when sftp is used as a transfer protocol.  We have seen this error
being caused by verbose `.bashrc` files (or other login files), which confuses
sftp startup.  Please make sure that any parts of the bashrc which print
information are only executed on interactive shell (i.e., on shells which have
a prompt set as `$PS1`). The snippet below shows how to do that:

.. code-block:: bash

    if [ ! -z "$PS1" ]
    then
      echo "hello $USER"
      date
    fi
    

Pop-up "Do you want the application python to accept incoming network connections?" on macOS
--------------------------------------------------------------------------------------------

Currently, we do not support RADICAL-Pilot on macOS. If macOS support is
critical for you, please open 
`an issue <https://github.com/radical-cybertools/radical.pilot/issues>`_.

.. This is coming from the firewall on your Mac. You can either:
..    - click "Allow" (many times) 
..    - disable your firewall (temporarily)
..    - Sign the application per instructions here: http://apple.stackexchange.com/a/121010


Error "Could not detect shell prompt (timeout)"
-----------------------------------------------

We support `sh` and `bash` as login shells on the target machines.  Please try
to switch to those shells if you use others like `zsh` and `csh/tcsh`.  If you
need other shells supported, please open 
`an issue <https://github.com/radical-cybertools/radical.pilot/issues>`_.

Prompt detecting behavior can be improved by calling `touch $HOME/.hushlogin`
on the target machine, which will suppress some system messages on login. If
the problem persists, please open 
`an issue <https://github.com/radical-cybertools/radical.pilot/issues>`_.

Details: we implement a rather cumbersome screen scraping via an interactive
ssh session to get onto the target machine, instead of using `paramiko` or
other modules that proven to be too buggy and unstable.  This gives us better
performance, but most importantly, this gives us support for `gsissh`, which
we did not find available in any other package so far.


Number of concurrent RADICAL-Pilot scripts that can can be executed
-------------------------------------------------------------------

From a RADICAL-Pilot perspective, there is no limit, but as SSH is used to
access many systems, there is a resource-specific limit of the number of
concurrent SSH connections one can make.