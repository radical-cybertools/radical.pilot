
.. _chapter_user_guide_11:

**********************************
Using containerized compute units
**********************************

A container is a standard unit of software that packages up code and all 
its dependencies so the application runs quickly and reliably from one 
computer environment to another. A container image is a lightweight standalone, 
executable package of software that includes everything needed to run an 
application: code, runtime, system tools, system libraries and settings.

The image becomes a container at runtime, and said containerized software will 
always run the same, regardless of the infrastructure, ensuring that it works 
uniformly despite differences for instance between development and staging.

What is Singularity?
-------------------

Singularity is a container runtime that favors integration while still 
preserving security restrictions on the container, and providing reproducible 
images. Furthermore, it enables users to have full control of their environment. 
Singularity containers can be used to package entire scientific workflows, 
software and libraries, and even data. This means that you do not have to ask 
your cluster administrator to install anything for you, you can put it in a 
Singularity container and run.

In order to see the Initial Presentation on Containers, please visit `open a ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_
If you want to take a deeper look into containers and Singularity, please refer to the following document:
