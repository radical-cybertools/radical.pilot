.. _chapter_example_gettinstarted:

***************
Getting Started 
***************

After you have successfully installed SAGA-Pilot (see chapter :ref:`chapter_installation`) on your system, let's write our first SAGA-Python application. 

[Description of what the getting started example does]

 

Loading the Module
------------------

In order to use SAGA-Pilot in your Python application, you need to import the ``sagapilot`` module.

.. code-block:: python

    import sagapilot

You can check / print the version of your SAGA-Pilot installation via the ``version`` property

.. code-block:: python

    print sagapilot.version

Creating a Session
------------------

A :class:`sagapilot.Session` is the root object for all other objects in SAGA-Pilot. You can think
of it as a *tree* or a *directory structure* with a Session as root:

.. code-block:: bash

    [Session]
    |
    |---- SSHCredential
    |---- ....
    |
    |---- [PilotManager]
    |     |
    |     |---- ComputePilot
    |     |---- ComputePilot
    |  
    |---- [UnitManager]
    |     |
    |     |---- ComputeUnit
    |     |---- ComputeUnit
    |     |....
    |
    |---- [UnitManager]
    |     |
    |     |....
    |
    |....




