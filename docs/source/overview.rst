
.. _chapter_overview:

************************
RADICAL-Pilot - Overview
************************

This section will provide a conceptual overview about RADICAL-Pilot (RP).  You
will learn what problems RP tries to solve -- and importantly also what problems
it will *not* solve.  You will also be introduced to some vocabulary, and the
overall RP architecture and operation.  

We will keep the information on a very general level, and will avoid any details
which will not contribute to the user experience.  Having said that, feel free
to skip ahead to the :ref:`chapter_user_guide` if you are more interested in
directly diving into the thick of using RP!


What Problems does RP solve?
============================

RP attempts to support in running applications on distributed resources, and
focuses on two aspects:

  * abstract the heterogeneity of distributed resources, so that running
    applications is uniform across them, from a users perspective; 

  * support the efficient execution of large numbers of concurrent or sequential
    application instances.



What is a Pilot?
================

The `Pilot` in RADICAL-Pilot stands for a job-container like construct which
manages a part (`slice`) of a remote resource on the user's (or application's)
behalf, and which executes sequences of ComputeUnits on that resource slice.

RP applications will, in general, first define a set of such pilots, ie. the set
of target resources, the size of the resource slice to manage, etc), and then
submit those pilots to the resources.  Once the pilots are active, the
application can send them `ComputeUnits` (see below) for execution.


What is a Compute Unit (CU)?
============================

An RP ComputeUnit (CU, or 'unit') represents a self-contained, executable part
of the application's workload.  A CU is described by the following attributes
(for details, check out the :class:`API documentation <radical.pilot.ComputeUnitDescription>`):

  * `executable`    : the name of the executable to be run on the target machines
  * `arguments`     : a list of argument strings to be passed to the executable
  * `environment`   : a dictionary of environment variable/value pairs to be set
                      before unit execution
  * `input_staging` : a set of staging directives for input data
  * `output_staging`: a set of staging directives for output data


How about data?
===============

Data management is important for executing CUs, both in providing input data,
and staging/sharing output data.  RP has different means to handle data, and
they are specifically covered in sections
:ref:`in <chapter_user_guide_06>`
:ref:`the <chapter_user_guide_07>`
:ref:`UserGuide <chapter_user_guide_08>`.



What is a scheduler?  Why are there multiple schedulers?
========================================================

Why do I need a MongoDB to run RP?
==================================

How do I know what goes on in the pilot? With my CUs?
=====================================================

