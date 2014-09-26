
.. _chapter_release_notes:

*************
Release Notes
*************

Release 0.19
------------

* New Features / Fixed Problems
    * improved MPI support for ComputeUnits
    * improved documentation and tutorials
    * faster file transfer
    * support sharing of staged data among ComputeUnits
    * new, expanded data staging directives (backward compatible with previous implementaion)
    * support for re-usable pilot virtualenv
    * better support for unicode in ComputeUnit stdout/stderr
    * improved shutdown, cancellation, and termination routines
    * backfilling scheduler
    * command line tools for session inspection and cleanup
    * beta, support for plotting of session profils and statistics
    * radical.pilot.Session now inherits from saga.Session, thus making SAGA context types for username/password, ssh, myproxy and x509/gsissh avaiable
    * session can be closed in state callbacks
    * better error handling in worker and agent threads
    * faster ComputeUnit startup (i.e. higher throughput)
    * for a list of closed tickets, please refer to the complete 
      `list of closed tickets for this release 
      <https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aclosed+milestone%3AMS-8>`_.

* Notable Known Problems
    * :issue:`310`: application and pilot shutdown on keyboard interrupt or on exceptions is
      still not reliable
    * :issue:`193`: mongodb access is not secured by default
    * :issue:`157`: session and pilot sandbox cleanup seem not to be reliable, yet
    * :issue:`129`: Callbacks dont' get called on object creation Bug
    * for a list of known problems, please refer to the complete 
      `list of open tickets <https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aopen+>`_



