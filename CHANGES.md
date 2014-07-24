0.18 Release                                            July 22. 2014
---------------------------------------------------------------------

* The Milestone 7 release (MS.7)
* Closed Tickets:

  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=13&state=closed


0.17 Release                                            June 18. 2014
---------------------------------------------------------------------

Bugfix release - fixed file permissions et al. :/


0.16 Release                                            June 17. 2014
---------------------------------------------------------------------

Bugfix release - fixed file permissions et al.


0.15 Release                                            June 12. 2014
---------------------------------------------------------------------

Bugfix release - fixed distribution MANIFEST:

https://github.com/radical-cybertools/radical.pilot/issues/174


0.14 Release                                            June 11. 2014
---------------------------------------------------------------------

Closed Tickets:

* https://github.com/radical-cybertools/radical.pilot/issues?milestone=16&state=closed

New Features

* Experimental pilot-agent for Cray systems
* New multi-core agent with MPI support
* New ResourceConfig mechanism does not reuquire the user to add 
  resource configurations explicitly. Resources can be configured 
  programatically on API-level.

API Changes:

* ComputeUnitDescription.working_dir_priv removed
* Extended state model
* resource_configurations parameter removed from PilotManager c`tor


0.13 Release                                             May 19. 2014
---------------------------------------------------------------------

* ExTASY demo release 
* Support for project / allocation
* Updated / simplified resource files
* Refactored bootstrap mechnism


0.12 Release                                             May 09. 2014
---------------------------------------------------------------------

* Updated resource files
* Updated state model
* Closed tickets: 
  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=12&state=closed


0.11 Release                                            Apr. 29. 2014
---------------------------------------------------------------------

* Fixes error in state history reporting

0.10 Release                                            Apr. 29. 2014
---------------------------------------------------------------------

* Support for state transition introspection via CU/Pilot state_history
* Cleaned up an streamlined Input and Outpout file transfer workers
* Support for interchangeable pilot agents
* Closed tickets:
  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=11&state=closed


0.9 Release                                             Apr. 16. 2014
---------------------------------------------------------------------

* Support for output file staging
* Streamlines data model
* More loosely coupled components connected via DB queues
* Closed tickets:
  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=10&state=closed


0.8 Release                                             Mar. 24. 2014
---------------------------------------------------------------------

* Renamed codebase from sagapilot to radical.pilot
* Added explicit close() calls to PM, UM and Session.
* Cloesed tickets:
  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=9&state=closed


0.7 Release                                             Feb. 25. 2014
---------------------------------------------------------------------

* Added support for callbacks 
* Added support for input file transfer !
* Closed tickets:
  - https://github.com/radical-cybertools/radical.pilot/issues?milestone=8&state=closed


0.6 Release                                             Feb. 24. 2014
---------------------------------------------------------------------

* BROKEN RELEASE


0.5 Release                                             Feb. 06. 2014
---------------------------------------------------------------------

* Tutorial 2 release (Github only)
* Added support for multiprocessing worker
* Support for CU stdout and stderr transfer via MongoDB GridFS
* Closed tickets:
  - https://github.com/saga-project/saga-pilot/issues?milestone=7&page=1&state=closed


0.4 Release 
---------------------------------------------------------------------

* Tutorial 1 release (Github only)
* Consistent naming (sagapilot instead of sinon)


0.1.3 Release 
---------------------------------------------------------------------

* Github only release: 
  pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot 

* Added logging
* Added security context handling 
* Closed tickets: 
  - https://github.com/saga-project/saga-pilot/issues?milestone=3&state=closed


0.1.2 Release 
---------------------------------------------------------------------

* Github only release: 
  pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot 

* Closed tickets: 
  - https://github.com/saga-project/saga-pilot/issues?milestone=4&state=closed
