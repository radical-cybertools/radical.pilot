

For a list of open issues and known problems, see:
https://github.com/radical-cybertools/radical.pilot/issues/

1.6.6  Release                                                        2021-05-18
--------------------------------------------------------------------------------

  - added flags to keep `prun` aware of gpus (PRTE2 LM)
  - add service node support
  - Bridges mpiexec confing fix
  - task level profiling now python independent
  - executor errors should not affect task bulks
  - revive ibrun support, include layout support
  - MPI standard prescribes -H, not -host
  - remove pilot staging area
  - reduce profiling verbosity
  - restore original env before task execution
  - scattered repex staging fixes
  - slurm env fixes
  - updated documentation for `PilotDescription` and `TaskDescription`


1.6.5  Release                                                        2021-04-14
--------------------------------------------------------------------------------

  - added flag `exclusive` for tags (in task description, default `False`)
  - Adding Bridges2 and Comet
  - always specifu GPU number on srun
  - apply RP+* env vars to raptor tasks
  - avoid a termination race
  - Summit LFS config and JSRUN integration tests
  - gh workflows and badges
  - ensure that RU lock names are unique
  - fixed env creation command and updated env setup check processes
  - fixed launch command for PRTE2 LM
  - fix missing event updates
  - fix ve isolation for prep_env
  - keep track of tagged nodes (no nodes overlapping between different tags)
  - ensure conda activate works
  - allow output staging on failed tasks
  - python 2 -> 3 fix for shebangs
  - remove support for add_resource_config
  - Stampede2 migrates to work2 filesystem
  - update setup module (use `python3`)
  

1.6.3  Hotfix Release                                                 2021-04-03
--------------------------------------------------------------------------------

  - fix uid assignment for managers


1.6.2  Hotfix Release                                                 2021-03-26
--------------------------------------------------------------------------------

  - switch to pep-440 for sdist and wheel versioning, to keep pip happy


1.6.1  Release                                                        2021-03-09
--------------------------------------------------------------------------------

  - support for Andes@ORNL, obsolete Rhea@ORNL
  - add_pilot() also accepts pilot dict
  - fixed conda activation for PRTE2 config (Summit@ORNL)
  - fixed partitions handling in LSF_SUMMIT RM
  - reorganized DVM start process (prte2)
  - conf fixes for comet
  - updated events for PRTE2 LM
  - integration test for Bridges2
  - prepare partitioning


1.6.0  Release                                                        2021-02-13
--------------------------------------------------------------------------------

  - rename ComputeUnit             -> Task
  - rename ComputeUnitDescription  -> TaskDescription
  - rename ComputePilot            -> Pilot
  - rename ComputePilotDescription -> PilotDescription
  - rename UnitManager             -> TaskManager
  - related renames to state and constant names etc
  - backward compatibility for now deprecated names
  - preparation for agent partitioning (RM)
  - multi-DVM support for PRTE.v1 and PRTE.v2
  - RM class tests
  - Bridges2 support
  - fix to co-scheduling tags
  - fix handling of IP variable in bootstrap
  - doc and test updates, linter fixes, etc
  - update scheduler tag types


1.5.12 Release                                                        2021-02-02
--------------------------------------------------------------------------------

  - multi-dvm support
  - cleanup of raptor
  - fix for bootstrap profiling
  - fix help string in bin/radical-pilot-create-static-ve
  - forward compatibility for tags
  - fix data stager for multi-pilot case
  - parametric integration tests
  - scattered fixes for raptor and sub-agent profiling
  - support new resource utilization plots


1.5.11 Release                                                        2021-01-19
--------------------------------------------------------------------------------

  - cleanup pypi tarball


1.5.10 Release                                                        2021-01-18
--------------------------------------------------------------------------------

  - gpu related fixes (summit)
  - avoid a race condition during termination
  - fix bootstrapper timestamps
  - fixed traverse config
  - fix nod counting for FORK rm
  - fix staging context
  - move staging ops into separate worker
  - use C locale in bootstrapper


1.5.8 Release                                                         2020-12-09
--------------------------------------------------------------------------------

  - improve test coverage
  - add env isolation prototype and documentation
  - change agent launcher to ssh for bridges
  - fix sub agent init
  - fix Cheyenne support
  - define an intel-friendly bridges config
  - add environment preparation to pilot
  - example fixes
  - fixed procedure of adding resource config to the session
  - fix mpiexec_mpt LM
  - silence scheduler log
  - removed resource aliases
  - updated docs for resource config
  - updated env variable RADICAL_BASE for a job description
  - work around pip problem on Summit


1.5.7 Release                                                         2020-10-30
--------------------------------------------------------------------------------

  - Adding init files in all test folders
  - document containerized tasks
  - Fix #2221
  - Fix read_config
  - doc fixes / additions
  - adding unit tests, component tests
  - remove old examples
  - fixing rp_analytics #2114
  - inject workers as MPI task
  - remove debug prints
  - mpirun configs for traverse, stampede2
  - ru.Config is responsible to pick configs from correct paths
  - test agent execution/base
  - unit test for popen/spawn #1881


1.5.4 Release                                                         2020-10-01
--------------------------------------------------------------------------------

  - fix jsrun GPU mapping


1.5.4 Release                                                         2020-09-14
--------------------------------------------------------------------------------

  - Arbitrary udurations for consumed resources
  - Fix unit tests
  - Fix python stack on Summit
  - add module test
  - added PRTE2 for PRRTEv2
  - added attribute for SAGA job description using env variable (SMT)
  - added config for PRRTE launch method at Frontera
  - added test for PRTE2
  - added test for rcfg parameter SystemArchitecture
  - allow virtenv_mode=local to reuse client ve
  - bulk communication for task overlay
  - fixed db close/disconnect method
  - fixed tests and pylint
  - PRTE fixes / updates
  - remove "debug" rp_version remnant


1.5.2 Hotfix Release                                                  2020-08-11
--------------------------------------------------------------------------------

  - add/fix RA prof metrics
  - clean dependencies
  - fix RS file system cache

      
1.5.1 Hotfix Release                                                  2020-08-05
--------------------------------------------------------------------------------

  - added config parameter for MongoDB tunneling
  - applied exception chaining
  - filtering for login/batch nodes that should not be considered (LSF RM)
  - fix for Resource Set file at JSRUN LM
  - support memory required per node at the RP level
  - added Profiler instance into Publisher and Subscriber (zmq.pubsub)
  - tests added and fixed
  - configs for Lassen, Frontera
  - radical-pilot-resources tool
  - document event model
  - comm bulking
  - example cleanup
  - fix agent base dir
  - Fix durations and add defaults for app durations
  - fixed flux import
  - fixing inconsistent nodelist error
  - iteration on task overlay
  - hide passwords on dburl reports / logs
  - multi-master load distribution
  - pep8
  - RADICAL_BASE_DIR -> RADICAL_BASE
  - remove private TMPDIR export - this fixes #2158
  - Remove SKIP_FAILED (unused)
  - support for custom batch job names
  - updated cuda hook for JSRUN LM
  - updated license file
  - updated readme
  - updated version requirement for python (min is 3.6)
  

1.4.1 Hotfix Release                                                  2020-06-09
--------------------------------------------------------------------------------

  - fix tmpdir mosconfiguration for summit / prrte


1.4.0 Release                                                         2020-05-12
--------------------------------------------------------------------------------

  - merge #2122: fixed `n_nodes` for the case when `slots` are set
  - merge #2123: fix #2121
  - merge #2124: fixed conda-env path definition
  - merge #2127: bootstrap env fix
  - merge #2133, #2138:  IBRun fixes
  - merge #2134: agent stage_in test1
  - merge #2137: agent_0 initialization fix
  - merge #2142: config update
  - add `deactivate` support for tasks
  - add cancelation example
  - added comet_mpirun to resource_xsede.json
  - added test for launch method "srun"
  - adding cobalt test
  - consistent process counting
  - preliminary FLUX support
  - fix RA utilization in case of no agent nodes
  - fix queue naming, prte tmp dir and process count
  - fix static ve location
  - fixed version discovery (srun)
  - cleanup bootstrap_0.sh
  - separate tacc and xsede resources
  - support for Princeton's Traverse cluster
  - updated IBRun tests
  - updated LM IBRun

      
1.3.0 Release                                                         2020-04-10
--------------------------------------------------------------------------------

  - task overlay + docs
  - iteration on srun placement
  - add env support to srun
  - theta config
  - clean up launcher termination guard against lower level termination errors
  - cobalt rm
  - optional output stager
  - revive ibrun support
  - switch comet FS


1.2.1 Hotfix Release                                                  2020-02-11
--------------------------------------------------------------------------------

  - scattered fixes cfor summit

      
1.2.0 Release                                                         2020-02-11
--------------------------------------------------------------------------------

  - support for bulk callbacks
  - fixed package paths for launch methods (radical.pilot.agent.launch_method)
  - updated documentation references
  - raise minimum Python version to 3.6
  - local submit configuration for Frontera
  - switch frontera to default agent cfg
  - fix cray agent config
  - fix issue #2075 part 2

      
1.1.1 Hotfix Release                                                  2020-02-11
--------------------------------------------------------------------------------

  - fix dependency version for radical.utils

      
1.1 Release                                                           2020-02-11
--------------------------------------------------------------------------------

  - code cleanup


1.0.0   Release                                                       2019-12-24
--------------------------------------------------------------------------------

  - transition to Python3
  - migrate Rhea to Slurm
  - ensure PATH setting for sub-agents
  - CUDA is now handled by LM
  - fix / improve documentation
  - Sched optimization: task lookup in O(1)
  - Stampede2 prun config
  - testing, flaking, linting and travis fixes
  - add `pilot.stage_out` (symmetric to `pilot.stage_in`)
  - add noop sleep executor
  - improve prrte support
  - avoid state publish during idle times
  - cheyenne support
  - default to cont scheduler
  - configuration system revamp
  - heartbeat based process management
  - faster termination
  - support for  Frontera
  - lockfree scheduler base class
  - switch to RU ZMQ layer

      
0.90.1  Release                                                       2019-10-12
--------------------------------------------------------------------------------

  - port pubsub hotfix

      
0.90.0  Release                                                       2019-10-07
--------------------------------------------------------------------------------

  - transition to Python3

      
0.73.1  Release                                                       2019-10-07
--------------------------------------------------------------------------------

  - Stampede-2 support


0.72.2  Hotfix Release                                                2019-09-30
--------------------------------------------------------------------------------

  - fix sandbox setting on absolute paths


0.72.0  Release                                                       2019-09-11
--------------------------------------------------------------------------------

  - implement function executor
  - implement / improve PRTE launch method
  - PRTE profiling support (experimental)
  - agent scheduler optimizations
  - summit related configuration and fixes
  - initial frontera support
  - archive ORTE
  - increase bootstrap timeouts
  - consolidate MPI related launch methods
  - unit testing and linting
  - archive ORTE, issue #1915
  - fix `get_mpi_info` for Open MPI
  - base classes to raise notimplemented. issue #1920
  - remove outdated resources
  - ensure that pilot env reaches func executor
  - ensureID uniqueness across processes
  - fix inconsistencies in task sandbox handling
  - fix gpu placement alg
  - fix issue #1910
  - fix torque nodefile name and path
  - add metric definitions in RA support
  - make DB comm bulkier
  - expand resource configs with pilot description keys
  - better tiger support
  - add NOOP scheduler
  - add debug executor


0.70.3  Hotfix Release                                                2019-08-02
--------------------------------------------------------------------------------

  - fix example and summit configuration


0.70.2  Hotfix Release                                                2019-07-31
--------------------------------------------------------------------------------

  - fix static ve creation for Tiger (Princeton)


0.70.1  Hotfix Release                                                2019-07-30
--------------------------------------------------------------------------------

  - fix configuration for Tiger (Princeton)

      
0.70.0  Release                                                       2019-07-07
--------------------------------------------------------------------------------

  - support summitdev, summit @ ORNL (JSRUN, PRTE, RS, ERF, LSF, SMT)
  - support tiger @ princeton (JSRUN)
  - implement NOOP scheduler
  - backport application communicators from v2
  - ensure session close on some tests
  - continous integration: pep8, travis, increasing test coverage
  - fix profile settings for several LMs
  - fix issue #1827
  - fix issue #1790
  - fix issue #1759
  - fix HOMBRE scheduler
  - remove cprof support
  - unify mpirun / mpirun_ccmrun
  - unify mpirun / mpirun_dplace
  - unify mpirun / mpirun_dplace
  - unify mpirun / mpirun_dplace
  - unify mpirun / mpirun_mpt
  - unify mpirun / mpirun_rsh


0.63.0  Release                                                       2019-06-25
--------------------------------------------------------------------------------

  - support for summit (experimental, jsrun + ERF)
  - PRRTE support (experimental, summit only)
  - many changes to the test setup (pytest, pylint, flake8, coverage, travis)
  - support for Tiger (adds SRUN launch method)
  - support NOOP scheduler
  - support application level communication
  - support ordered scheduling of tasks
  - partial code cleanup (coding guidelines)
  - simplifying MPI base launch methods
  - support for resource specific SMT settings
  - resource specific ranges of cores/threads can now be blocked from use
  - ORTE support is doscontinued
  - fixes in hombre scheduler
  - improvements on GPU support
  - fix in continuous scheduler which caused underutilization on heterogeneous
    tasks
  - fixed: #1758, #1764, #1792, #1790, #1827, #187


0.62.0  Release                                                       2019-06-08
--------------------------------------------------------------------------------

  - add unit test
  - trigger tests
  - remove obsolete fifo scheduler (use the ordered scheduler instead)
  - add ordered scheduler
  - add tiger support
  - add ssh access to cheyenne
  - cleanup examples
  - fix dplace support
  - support app specified task sandboxes
  - fix pilot statepush over tunnels
  - fix titan ve creation, add new static ve
  - fix for cheyenne


0.61.0  Release                                                       2019-05-07
--------------------------------------------------------------------------------

  - add travis support, test cleanup
  - ensure safe bootstrapper termination on faulty installs
  - push node_list to mongodb for analytics
  - fix default dburl
  - fix imports in tests
  - remove deprecated special case in bootstrapper


0.60.1  Hotfix                                                        2019-04-12
--------------------------------------------------------------------------------

  - work around a pip install problem


0.60.0  Release                                                       2019-04-10
--------------------------------------------------------------------------------

  - add issue template
  - rename RP_PILOT_SBOX to RP_PILOT_STAGING and expose to tasks
  - fix bridges default partition (#1816)
  - fix #1826
  - fix off-by-one error on task state check
  - ignore failing DB disconnect
  - follow rename of saga-python to radical.saga


0.50.23 Release                                                       2019-03-20
--------------------------------------------------------------------------------

  - hotfix: use popen spawner for localhost


0.50.22 Release                                                       2019-02-11
--------------------------------------------------------------------------------

  - another fix LSF var expansion


0.50.21 Release                                                       2018-12-19
--------------------------------------------------------------------------------

  - fix LSF var expansion


0.50.20 Release                                                       2018-11-25
--------------------------------------------------------------------------------

  - fix Titan OMPI installation
  - support metdata for tasks
  - fix git error detection during setup


0.50.19 Release                                                       2018-11-15
--------------------------------------------------------------------------------

  - ensure profile fetching on empty tarballs


0.50.18 Release                                                       2018-11-13
--------------------------------------------------------------------------------

  - support for data locality aware scheduling


0.50.17 Release                                                       2018-10-31
--------------------------------------------------------------------------------

  - improve event documentation
  - support Task level metadata


0.50.16 Release                                                       2018-10-26
--------------------------------------------------------------------------------

  - add new shell spawner as popen replacement


0.50.15 Release                                                       2018-10-24
--------------------------------------------------------------------------------

  - fix recursive pilot staging             


0.50.14 Release                                                       2018-10-24
--------------------------------------------------------------------------------

  - add Cheyenne support - thanks Vivek!


0.50.13 Release                                                       2018-10-16
--------------------------------------------------------------------------------

  - survive if SAGA does not support job.name (#1744)


0.50.12 Release                                                       2018-10-12
--------------------------------------------------------------------------------

  - fix stacksize usage on BW


0.50.11 Release                                                       2018-10-09
--------------------------------------------------------------------------------

  - fix 'getting_started' example (no MPI)


0.50.10 Release                                                       2018-09-29
--------------------------------------------------------------------------------

  - ensure the correct code path in SAGA for Blue Waters


0.50.9  Release                                                       2018-09-28
--------------------------------------------------------------------------------

  - fix examples
  - fix issue #1715 (#1716)
  - remove Stampede's resource configs. issue #1711
  - supermic does not like `curl -1` (#1723)


0.50.8  Release                                                       2018-08-03
--------------------------------------------------------------------------------

  - make sure that CUD values are not None (#1688)
  - don't limit pymongo version anymore (#1687)


0.50.7  Release                                                       2018-08-01
--------------------------------------------------------------------------------

  - fix bwpy handling


0.50.6  Release                                                       2018-07-31
--------------------------------------------------------------------------------

  - fix curl tssl negotiation problem (#1683)


0.50.5  Release                                                       2018-07-30
--------------------------------------------------------------------------------

  - fix default values for process and thread types (#1681)
  - fix outdated links in ompi deploy script
  - fix/issue 1671 (#1680)
  - fix scheduler config checks (#1677)


0.50.4  Release                                                       2018-07-13
--------------------------------------------------------------------------------

  - set oversubscribe default to True


0.50.3  Release                                                       2018-07-11
--------------------------------------------------------------------------------

  - disable rcfg expnsion


0.50.2  Release                                                       2018-07-08
--------------------------------------------------------------------------------

  - fix relative tarball unpack paths


0.50.1  Release                                                       2018-07-05
--------------------------------------------------------------------------------

  - GPU support
  - many bug fixes


0.47.14 Release                                                       2018-06-13
--------------------------------------------------------------------------------

  - fix recursive output staging


0.47.13 Release                                                       2018-06-02
--------------------------------------------------------------------------------

  - catch up with RU log, rep and prof settings


0.47.12 Release                                                       2018-05-19
--------------------------------------------------------------------------------

  - ensure that tasks are started in their own process group, to ensure clean
    cancellation semantics.


0.47.11 Release                                                       2018-05-08
--------------------------------------------------------------------------------

  - fix schemas on BW (local orte, local aprun)


0.47.10 Release                                                       2018-04-19
--------------------------------------------------------------------------------

  - fix #1602


0.47.9 Release                                                        2018-04-18
--------------------------------------------------------------------------------

  - fix default scheduler for localhost


0.47.8 Release                                                        2018-04-16
--------------------------------------------------------------------------------

  - hotfix to catch up with pypi upgrade


0.47.7 Release                                                        2018-04-15
--------------------------------------------------------------------------------

  - bugfix related to radical.entk #255


0.47.6 Release                                                        2018-04-12
--------------------------------------------------------------------------------

  - bugfix related to #1590


0.47.5 Release                                                        2018-04-12
--------------------------------------------------------------------------------

  - make sure a dict object exists even on empty env settings (#1590)


0.47.4 Release                                                        2018-03-20
--------------------------------------------------------------------------------

  - fifo agent scheduler (#1537)
  - hombre agent scheduler (#1536)
  - Fix/issue 1466 (#1544)
  - Fix/issue 1501 (#1541)
  - switch to new OMPI deployment on titan (#1529)
  - add agent configuration doc (#1540)


0.47.3 Release                                                        2018-03-20
--------------------------------------------------------------------------------

  - add resource limit test
  - add tmp cheyenne config
  - api rendering proposal for partitions
  - fix bootstrap sequence (BW)
  - tighten bootstrap process, add documentation


0.47.2 Release                                                        2018-02-28
--------------------------------------------------------------------------------

  - fix issue 1538
  - fix issue 1554
  - expose profiler to LM hooks (#1522)
  - fix bin names (#1549)
  - fix event docs, add an event for symmetry (#1527)
  - name attribute has been changed to uid, fixes issue #1518
  - make use of flags consistent between RP and RS (#1547)
  - add support for recursive data staging (#1513. #1514)  (JD, VB, GC)
  - change staging flags to integers (inherited from RS)
  - add support for bulk data transfer (#1512) (IP, SM)


0.47 Release                                                          2017-11-19
--------------------------------------------------------------------------------

  - Correctly added 'lm_info.cores_per_node' SLURM
  - Torque RM now respects config settings for cpn
  - Update events.md
  - add SIGUSR2 for clean termination on SGE
  - add information about partial event orders
  - add issue demonstrators
  - add some notes on cpython issue demonstrators
  - add xsede.supermic_orte configuration
  - add xsede.supermic_ortelib configuration
  - apply RU's managed process to termination stress test
  - attempt to localize aprun tasks
  - better hops for titan
  - better integration of Task script and app profs
  - catch up with config changes for local testing
  - centralize URL derivation for pilot job service endpoints, hops, and sandboxes
  - clarify use of namespace vs. full qualified URL in the context of RP file staging
  - clean up config management, inheritance
  - don't fetch json twice
  - ensure that profiles are flushed and packed correctly
  - fail missing pilots on termination
  - fix AGENT_ACTIVE profile timing
  - fix close-session purge mode
  - fix cray agent config, avoid termination race
  - fix duplicated transition events
  - fix osg config
  - fix #1283
  - fixing error from bootstrapper + aprun parsing error
  - force profile flush earlier
  - get cpn for ibrun
  - implement session.list_resources() per #1419
  - make sure a canceled pilot stays canceled
  - make cb return codes consistent
  - make sure profs are flushed on termination
  - make sure the tmgr only pulls tasks its interested in
  - profile mkdir
  - publish resource_details (incl. lm_info) again
  - re-add a profile flag to advance()
  - remove old controllers
  - remove old files
  - remove uid clashes for sub-agent components and components in general
  - setup number of cores per node on stampede2
  - smaller default pilot size for supermic
  - switch to ibrun for comet_ssh
  - track task drops
  - use js hop for untar
  - using new process class
  - GPU/CPU pinning test is now complete, needs some env settings in the launchers


0.46.2 Release                                                        2017-09-02
--------------------------------------------------------------------------------

  - hotfix for #1426 - thanks Iannis!


0.46.1 Release                                                        2017-08-23
--------------------------------------------------------------------------------

  - hotfix for #1415


Version 0.46                                                          2017-08-11
--------------------------------------------------------------------------------

  - TODO


0.45.3 Release                                                        2017-05-09
--------------------------------------------------------------------------------

  - Documentation update for the BW tutorial


0.45.1 Release                                                        2017-03-05
--------------------------------------------------------------------------------

  - NOTE: OSG and ORTE_LIB on titan are considered unsupported.  You can enable
          those resources for experiments by setting the `enabled` keys in the
          respective config entries to `true`.

  - hotfix the configurations markers above


0.45 Release                                                          2017-02-28
--------------------------------------------------------------------------------

  - NOTE: OSG and ORTE_LIB on titan are considered unsupported.  You can enable
          those resources for experiments by removing the comment markers from
          the respective resource configs.

  - Adapt to new orte-submit interface.
  - Add orte-cffi dependency to bootstrapper.
  - Agent based staging directives.
  - Fixes to various resource configs
  - Change orte-submit to orterun.
  - Conditional importing of executors. Fixes #926.
  - Config entries for orte lib on Titan.
  - Corrected environment export in executing POPEN
  - Extend virtenv lock timeout, use private rp_installs by default
  - Fix non-mpi execution analogous to #975.
  - Fix/issue 1226 (#1232)
  - Fresh orte installation for bw.
  - support more OSG sites
  - Initial version of ORTE lib interface.
  - Make cprofiling of scheduler conditional.
  - Make list of cprofile subscribers configurable.
  - Move env safekeeping until after the pre bootstrap.
  - Record OSG site name in mongodb.
  - Remove bash'isms from shell script.
  - pylint motivated cleanups
  - Resolving issue #1211.
  - Resource and example config for Shark at LUMC.
  - SGE changes for non-homogeneous nodes.
  - Use ru.which
  - add allegro.json config file for FUB allegro cluster
  - add rsh launch method
  - switch to gsissh on wrangler
  - use new ompi installation on comet (#1228)
  - add a simple/stupid ompi deployment helper
  - updated Config for Stampede and YARN
  - fix state transition to UNSCHEDDULED to avoid repetition
    and invalid state ordering


0.44.1 Release                                                        2016-11-01
--------------------------------------------------------------------------------

  - add an agent config for cray/aprun all on mom node
  - add anaconda config for examples
  - gsissh as default for wrangler, stampede, supermic
  - add conf for spark n wrangler, comet
  - add docs to the cu env inject
  - expose spark's master url
  - fix Task env setting (stampede)
  - configuration for spark and anaconda
  - resource config entries for titan
  - disable PYTHONHOME setting in titan_aprun
  - dynamic configuration of spark_env
  - fix for gordon config
  - hardcode the netiface version until it is fixed upstream.
  - implement NON_FATAL for staging directives.
  - make resource config available to agent
  - rename scripts
  - update installation.rst
  - analytics backport
  - use profiler from RU
  - when calling a task state callback, missed states also trigger callbacks


0.43.1 Release                                                        2016-09-09
--------------------------------------------------------------------------------

  - hotfix: fix netifaces to version 0.10.4 to avoid trouble on BlueWaters


0.43 Release                                                          2016-09-08
--------------------------------------------------------------------------------

  - Add aec_handover for orte.
  - add a local confiuration for bw
  - add early binding eample for osg
  - add greenfield config (only works for single-node runs at the moment)
  - add PYTHONPATH to the vars we reset for Task envs
  - allow overloading of agent config
  - fix #1071
  - fix synapse example
  - avoid profiling of empty state transitions
  - Check of YARN start-all script. Raising Runtime error in case of error.
  - disable hwm altogether
  - drop clones *before* push
  - enable scheduling time measurements.
  - First commit for multinode YARN cluster
  - fix getip
  - fix iface detection
  - fix reordering of states for some update sequences
  - fix task cancellation
  - improve ve create script
  - make orte-submit aware of non-mpi CUs
  - move env preservation to an earlier point, to avoid pre-exec stuff
  - Python distribution mandatory to all confs
  - Remove temp agent config directory.
  - Resolving #1107
  - Schedule behind the real task and support multicore.
  - SchedulerContinuous -> AgentSchedulingComponent.
  - Take ccmrun out of bootstrap_2.
  - Tempfile is not a tempfile so requires explicit removal.
  - resolve #1001
  - Unbreak CCM.
  - use high water mark for ZMQ to avoid message drops on high loads


0.42 Release                                                          2016-08-09
--------------------------------------------------------------------------------

  - change examples to use 2 cores on localhost
  - Iterate documentation
  - Manual cherry pick fix for getip.


0.41 Release                                                          2016-07-15
--------------------------------------------------------------------------------

  - address some of error messages and type checks
  - add scheduler documentation simplify interpretation of BF oversubscription fix a log message
  - fix logging problem reported by Ming and Vivek
  - global default url, sync profile/logfile/db fetching tools
  - make staging path resilient against cwd changes
  - Switch SSH and ORTE for Comet
  - sync session cleanup tool with rpu
  - update allocation IDs


0.40.4 Release                                                        2016-05-18
--------------------------------------------------------------------------------

  - point release with more tutorial configurations


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
    - large Task support
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

  - timing fix to ensure task state ordering


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
  - support default callbacks on tasks and pilots
  - use a config for examples
  - add lrms shutdown hook for ORTE LM
  - various updates to examples and documentation
  - create logfile and profile tarballs on the fly
  - export some RP env vars to tasks
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
  - several fixes and chages to the task state model (refer to documentation!)
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
  - fix, improve profiling of Task execution
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
  - cleanup of data staging, with better support for different schemas
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
  - adds timestamps to task and pilot logentries
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
  - fix handling of non-ascii data in task stdio
  - simplify switching of access schemas on pilot submission
  - disable pilot virtualenv for task execution
  - MPI support for DaVinci
  - performance optimizations on file transfers, task sandbox setup
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

  - TaskDescription.working\_dir\_priv removed
  - Extended state model
  - resource\_configurations parameter removed from PilotManager c`tor


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

  - Support for state transition introspection via Task/Pilot state\_history
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
  - Support for Task stdout and stderr transfer via MongoDB GridFS
  - Closed tickets:
    - https://github.com/saga-project/saga-pilot/issues?milestone=7&page=1&state=closed


0.4 Release
--------------------------------------------------------------------------------

  - Tutorial 1 release (Github only)
  - Consistent naming (sagapilot instead of sinon)


0.1.3 Release
--------------------------------------------------------------------------------

  - Github only release: pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot

  - Added logging
  - Added security context handling
  - Closed tickets:
    - https://github.com/saga-project/saga-pilot/issues?milestone=3&state=closed


0.1.2 Release
--------------------------------------------------------------------------------

  - Github only release: pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot

  - Closed tickets:
    - https://github.com/saga-project/saga-pilot/issues?milestone=4&state=closed

        
--------------------------------------------------------------------------------

