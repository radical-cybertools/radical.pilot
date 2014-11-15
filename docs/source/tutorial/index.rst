.. _chapter_tutorial:

********
Tutorial
********

This tutorial will walk you through the basic features of RADICAL-Pilot. Several 
examples illustrate some common usage patterns in distributed environments.

**Prerequisites:**

* You are familiar with Linux or UNIX.
* You can read and write Python code.
* You can use SSH and understand how public and private keys work.
* You understand the basic concepts of distributed computing.

**You will learn how to:**

* Submit multiple jobs through a 'pilot job' to your local workstation as well as a remote machine.
* Write programs that run multiple jobs concurrently, sequentially, or both, 
  based on their requirements and dependencies.
* How to specify input files for tasks.
* MPI tasks are different from regular tasks.

**Contents:**

.. image:: ../images/stooges.png


.. toctree::
   :maxdepth: 1

   simple_bot.rst
   chained_tasks.rst
   coupled_tasks.rst
   mpi_tasks.rst
