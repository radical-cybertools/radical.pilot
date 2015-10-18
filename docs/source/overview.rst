
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



How about data?
===============

What is a scheduler?  Why are there multiple schedulers?
========================================================

Why do I need a MongoDB to run RP?
==================================

How do I know what goes on in the pilot? With my CUs?
=====================================================

