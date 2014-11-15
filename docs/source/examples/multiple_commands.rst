.. _chapter_example_multiple_commands:

***************************************************
Executing Multiple Commands in a Single ComputeUnit  
***************************************************

There are scenarios in which you might want to execute more than one command in
a  ComputeUnit. For example, you might have to create and change into a
directory or load a module or a specific version of a software package before
you call your *main* executable.

In RADICAL-Pilot this can be easily achieved by using ``/bin/bash`` as the 
executable in the :class:`radical.pilot.ComputeUnitDescription` and either pass
a shell script directly as a string argument or transfer a shell script file
as part of the ComputeUnit. The former works well for a small set of simple 
commands, while the second works best for more complex scripts. 

Using ``/bin/bash`` as Executable
---------------------------------

TODO - explain -c and -l

.. code-block:: python
    
    cu = radical.pilot.ComputeUnitDescription()
    cu.executable  = "/bin/bash"
    cu.arguments   = ["-l", "-c", " this && and && that """]
    cu.cores       = 1
 

Using a Shell-Script File
-------------------------

TODO

.. code-block:: python

    TODO
