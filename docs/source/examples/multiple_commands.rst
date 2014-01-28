.. _chapter_example_multiple_commands:

***************************************************
Executing Multiple Commands in a Single ComputeUnit  
***************************************************

Using ``/bin/bash`` as Executable
---------------------------------

TODO - explain -c and -l

.. code-block:: python
    
    cu = sagapilot.ComputeUnitDescription()
    cu.executable  = "/bin/bash"
    cu.arguments   = ["-c", "-l", " this && and && that """]
    cu.cores       = 1
 

Using a Shell-Script File
-------------------------

TODO

.. code-block:: python

    TODO
