
  - For a list of bug fixes, see 
    https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
  - For a list of open issues and known problems, see
    https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aissue+is%3Aopen+


0.40.3 Release                                                        2016-05-17
--------------------------------------------------------------------------------

  - point release with tutorial configurations


0.40.2 Release                                                        2016-05-13
--------------------------------------------------------------------------------

  - hotfix to fix vnode parsing on archer


0.40.1 Release                                                        2016-02-11
--------------------------------------------------------------------------------

  - hotfix which makes sure agents don't report FAILED on cancel()


0.40 Release                                                          2016-02-03
--------------------------------------------------------------------------------

  - Really numberous changes, fixes and features, most prominently:
    - OSG support
    - Yarn support
    - new resource supported
    - ORTE used for more resources
    - improved examples, profiling
    - communication cleanup
    - large CU support
    - lrms hook fixes
    - agent code splitup


0.38 Release                                                          2015-12-22
--------------------------------------------------------------------------------

  - fix busy mongodb pull


0.37.10 Release                                                       2015-10-20
--------------------------------------------------------------------------------

  - config fix


0.37.9 Release                                                        2015-10-20
--------------------------------------------------------------------------------

  - Example fix


0.37.8 Release                                                        2015-10-20
--------------------------------------------------------------------------------

  - Allocation fix


0.37.7 Release                                                        2015-10-20
--------------------------------------------------------------------------------

  - Allocation fix


0.37.6 Release                                                        2015-10-20
--------------------------------------------------------------------------------

  - Documentation


0.37.5 Release                                                        2015-10-19
--------------------------------------------------------------------------------

  - timing fix to ensure unit state ordering


0.37.3 Release                                                        2015-10-19
--------------------------------------------------------------------------------

  - small fixes, doc changes


0.37.2 Release                                                        2015-10-18
--------------------------------------------------------------------------------

  - fix example installation


0.37.1 Release                                                        2015-10-18
--------------------------------------------------------------------------------

  - update of documentation and examples
  - some small fixes on shutdown installation


0.37 Release                                                          2015-10-15
--------------------------------------------------------------------------------

  - change default spawner to POPEN
  - use hostlist to avoid mpirun* limitations
  - support default callbacks on units and pilots
  - use a config for examples
  - add lrms shutdown hook for ORTE LM 
  - various updates to examples and documentation
  - create logfile and profile tarballs on the fly 
  - export some RP env vars to units
  - Fix a mongodb race
  - internally unregister pilot cbs on shutdown 
  - move agent.stop to finally clause, to correctly react on signals 
  - remove RADICAL_DEBUG, use proper logger in queue, pubsub 
  - small changes to getting_started 
  - add APRUN entry for ARCHER. 
  - Updated APRUN config for ARCHER. Thanks Vivek! 
  - Use designated termination procedure for ORTE. 
  - Use statically compiled and linked OMPI/ORTE. 
  - Wait for its component children on termination
  - make localhost (ForkLRMS) behave like a resource with an inifnite number of cores 


0.36 Release                                                          2015-10-08
--------------------------------------------------------------------------------

  (the release notes also cover some changes from 0.34 to 0.35)

  - simplify agent process tree, process naming
  - improve session and agent termination
  - several fixes and chages to the unit state model (refer to documentation!)
  - fix POPEN state reporting
  - split agent component into individual, relocatable processes
  - improve and generalize agent bootstrapping
  - add support for dynamic agent layout over compute nodes
  - support for ORTE launch method on CRAY (and others)
  - add a watcher thread for the ORTE DVM
  - improves profiling support, expand to RP module
  - add various profiling analysis tools
  - add support for profile fetching from remote pilot sandbox
  - synchronize and recombine profiles from different pilots
  - add a simple tool to run a recorded session. 
  - add several utility classes: component, queue, pubsub
  - clean configuration passing from module to agent.
  - clean tunneling support
  - support different data frame formats for profiling
  - use agent infrastructure (LRMS, LM) for spawning sub-agents
  - allow LM to specify env vars to be unset. 
  - allow agent on mom node to use tunnel. 
  - fix logging to avoid log leakage from lower layers
  - avoid some file system bottlenecks
  - several resource specific configuration fixes (mostly stampede, archer, bw)
  - backport stdout/stderr/log retrieval
  - better logging of clone/drops, better error handling for configs 
  - fix, improve profiling of CU execution
  - make profile an object
  - use ZMQ pubsub and queues for agent/sub-agent communication
  - decouple launch methods from scheduler for most LMs 
    NOTE: RUNJOB remains coupled!
  - detect disappearing orte-dvm when exit code is zero 
  - perform node allocation for sub-agents
  - introduce a barrier on agent startup 
  - fix some errors on shell spanwer (quoting, monotoring delays)
  - make localhost layout configurable via cpn 
  - make setup.py report a decent error when being used with python3 
  - support nodename lookup on Cray 
  - only mkdir in input staging controller when we intent to stage data 
  - protect agent cb invokation by lock
  - (re)add command line for profile fetching 
  - cleanup of data staging, with better support for different schemes
    (incl. GlobusOnline)
  - work toward better OSG support
  - Use netifaces for ip address mangling. 
  - Use ORTE from the 2.x branch. 
  - remove Url class


0.35.1 Release                                                        2015-09-29
--------------------------------------------------------------------------------

  - hotfix to use popen on localhost


0.35 Release                                                          2015-07-14
--------------------------------------------------------------------------------

  - numerous bug fixes and support for new resources


0.34 Release                                                          2015-07-14
--------------------------------------------------------------------------------

  - Hotfix release for an installation issue


0.33 Release                                                          2015-05-27
--------------------------------------------------------------------------------

  - Hotfix release for off-by-one error (#621)


0.32 Release                                                          2015-05-18
--------------------------------------------------------------------------------

  - Hotfix release for MPIRUN_RSH on Stampede (#572).


0.31 Release                                                          2015-04-30
--------------------------------------------------------------------------------

  - version bump to trigger pypi release update


0.30 Release                                                          2015-04-29
--------------------------------------------------------------------------------

  - hotfix to handle broken pip/bash combo on archer


0.29 Release                                                          2015-04-28
--------------------------------------------------------------------------------

  - hotfix to handle stale ve locks


0.28 Release                                                          2015-04-16
--------------------------------------------------------------------------------

  - This release contains a very large set of commits, and covers a fundamental
    overhaul of the RP agent (amongst others).  It also includes: 
    - support for agent profiling
    - removes a number of state race conditions
    - support for new backends (ORTE, CCM)
    - fixes for other backends
    - revamp of the integration tests


0.26 Release                                                          2015-04-08
--------------------------------------------------------------------------------

  - hotfix to cope with API changing pymongo release


0.25 Release                                                          2015-04-01
--------------------------------------------------------------------------------

  - hotfix for a stampede configuration change


0.24 Release                                                          2015-03-30
--------------------------------------------------------------------------------

  - More support for URLs in StagingDirectives (#489). 
  - Create parent directories of staged files. 
  - Only process entries for Output FTW, fixes #490. 
  - SuperMUC config change. 
  - switch from bson to json for session dumps 
  - fixes #451 
  - update resources.rst 
  - remove superfluous \n 
  - fix #438 
  - add documentation on resource config changes, closes #421 
  - .ssh/authorized_keys2 is deprecated since 2011 
  - improved intra-node SSH FAQ item 


0.23 Release                                                          2014-12-13
--------------------------------------------------------------------------------

  - fix #455


0.22 Release                                                          2014-12-11
--------------------------------------------------------------------------------

  - several state races fixed
  - fix to tools for session cleanup and purging
  - partial fix for pilot cancelation
  - improved shutdown behavior
  - improved hopper support
  - adapt plotting to changed slothistory format
  - make instructions clearer on data staging examples
  - addresses issue #216
  - be more resilient on pilot shutdown
  - take care of cancelling of active pilots
  - fix logic error on state check for pilot cancellation
  - fix blacklight config (#360)
  - attempt to cancel pilots timely...
  - as fallback, use PPN information provided by SAGA
  - hopper usues torque (thanks Mark!)
  - Re-fix blacklight config. Addresses #359 (again).
  - allow to pass application data to callbacks
  - threads should not be daemons...
  - workaround on failing bson encoding...
  - report pilot id on cu inspection
  - ignore caching errors
  - also use staging flags on input staging
  - stampede environment fix
  - Added missing stampede alias
  - adds timestamps to unit and pilot logentries
  - fix state tags for plots
  - fix plot style for waitq
  - introduce UNSCHEDULED state as per #233
  - selectable terminal type for plot
  - document pilot log env
  - add faq about VE problems on setuptools upgrade
  - allow to specify session cache files
  - added  configuration for BlueBiou (Thanks Jordane)
  - better support for json/bson/timestamp handling; cache mongodb data for stats, plots etc
  - localize numpy dependency
  - retire input_data and output_data
  - remove obsolete staging examples
  - address #410
  - fix another subtle state race


0.21 Release                                                          2014-10-29
--------------------------------------------------------------------------------

  - Documentation of MPI support
  - Documentation of data staging operations
  - correct handling of data transfer exceptions
  - fix handling of non-ascii data in unit stdio
  - simplify switching of access schemas on pilot submission
  - disable pilot virtualenv for unit execution
  - MPI support for DaVinci
  - performance optimizations on file transfers, unit sandbox setup
  - fix ibrun tmp file problem on stampede


0.19 Release                                                  September 12. 2014
--------------------------------------------------------------------------------

  - The Milestone 8 release (MS.8)
  - Closed Tickets:

    - https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aclosed+milestone%3AMS-8+


0.18 Release                                                       July 22. 2014
--------------------------------------------------------------------------------

  - The Milestone 7 release (MS.7)
  - Closed Tickets:

    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=13&state=closed


0.17 Release                                                       June 18. 2014
--------------------------------------------------------------------------------

Bugfix release - fixed file permissions et al. :/


0.16 Release                                                       June 17. 2014
--------------------------------------------------------------------------------

Bugfix release - fixed file permissions et al.


0.15 Release                                                       June 12. 2014
--------------------------------------------------------------------------------

Bugfix release - fixed distribution MANIFEST:

https://github.com/radical-cybertools/radical.pilot/issues/174


0.14 Release                                                       June 11. 2014
--------------------------------------------------------------------------------

Closed Tickets:

  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=16&state=closed

New Features

  - Experimental pilot-agent for Cray systems
  - New multi-core agent with MPI support
  - New ResourceConfig mechanism does not reuquire the user to add 
  resource configurations explicitly. Resources can be configured 
  programatically on API-level.

API Changes:

  - ComputeUnitDescription.working_dir_priv removed
  - Extended state model
  - resource_configurations parameter removed from PilotManager c`tor


0.13 Release                                                        May 19. 2014
--------------------------------------------------------------------------------

  - ExTASY demo release 
  - Support for project / allocation
  - Updated / simplified resource files
  - Refactored bootstrap mechnism


0.12 Release                                                        May 09. 2014
--------------------------------------------------------------------------------

  - Updated resource files
  - Updated state model
  - Closed tickets: 
    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=12&state=closed


0.11 Release                                                       Apr. 29. 2014
--------------------------------------------------------------------------------

  - Fixes error in state history reporting

0.10 Release                                                       Apr. 29. 2014
--------------------------------------------------------------------------------

  - Support for state transition introspection via CU/Pilot state_history
  - Cleaned up an streamlined Input and Outpout file transfer workers
  - Support for interchangeable pilot agents
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=11&state=closed


0.9 Release                                                        Apr. 16. 2014
--------------------------------------------------------------------------------

  - Support for output file staging
  - Streamlines data model
  - More loosely coupled components connected via DB queues
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=10&state=closed


0.8 Release                                                        Mar. 24. 2014
--------------------------------------------------------------------------------

  - Renamed codebase from sagapilot to radical.pilot
  - Added explicit close() calls to PM, UM and Session.
  - Cloesed tickets:
    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=9&state=closed


0.7 Release                                                        Feb. 25. 2014
--------------------------------------------------------------------------------

  - Added support for callbacks 
  - Added support for input file transfer !
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.pilot/issues?milestone=8&state=closed


0.6 Release                                                        Feb. 24. 2014
--------------------------------------------------------------------------------

  - BROKEN RELEASE


0.5 Release                                                        Feb. 06. 2014
--------------------------------------------------------------------------------

  - Tutorial 2 release (Github only)
  - Added support for multiprocessing worker
  - Support for CU stdout and stderr transfer via MongoDB GridFS
  - Closed tickets:
    - https://github.com/saga-project/saga-pilot/issues?milestone=7&page=1&state=closed


0.4 Release 
--------------------------------------------------------------------------------

  - Tutorial 1 release (Github only)
  - Consistent naming (sagapilot instead of sinon)


0.1.3 Release 
--------------------------------------------------------------------------------

  - Github only release: 
  pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot 

  - Added logging
  - Added security context handling 
  - Closed tickets: 
    - https://github.com/saga-project/saga-pilot/issues?milestone=3&state=closed


0.1.2 Release 
--------------------------------------------------------------------------------

  - Github only release: 
  pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot 

  - Closed tickets: 
    - https://github.com/saga-project/saga-pilot/issues?milestone=4&state=closed


--------------------------------------------------------------------------------


