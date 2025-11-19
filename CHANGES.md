# Release Notes

For a list of open issues and known problems, see:
 [https://github.com/radical-cybertools/radical.pilot/issues/](https://github.com/radical-cybertools/radical.pilot/issues/)

## Current

This is the latest release - if uncertain, use this release.

--------------------------------------------------------------------------------
### 1.103.2 Release                                                   2025-11-18

  - fix UID check


--------------------------------------------------------------------------------
### 1.103.0 Release                                                   2025-11-06

  - improve flux support
  - improve dragon support


--------------------------------------------------------------------------------
### 1.102.0 Release                                                   2025-05-28

  - adding resource config and doc for supported HPC
  - fix #3347
  - fix pytask docstring
  - move python version to 3.8
  - pass network configuration through to RU
  - raptor can execute sync/async funcs with (non)MPI workers
  - rtd fix
  - set iface as early as possible
  

--------------------------------------------------------------------------------
### 1.100.0 Release                                                   2025-03-26

  - add tutorial for app level scheduling
  - distutils are deprecated (bootstrapper)
  - extended functionality for `startup_timeout` attribute in TaskDescription
  - fix install of internals.rst
  - fix option check in mpiexec
  - implement startup signal
  - make it simpler to assign session IDs to experiments
  - make sure task tmpdirs exist
  - overwrite `NumaNode.as_dict()`
  - random changes to update old code snippets - please ignore in review
  - simplify NUMA scheduling, improve examples
  

--------------------------------------------------------------------------------
### 1.92.0 Release                                                    2025-02-25

  - allow oversubscribe on localhost
  - close control subscriber on `close()`
  - control PSI/J `exclusive` parameter for the LRM submission
  - don't use a gpu in examples - that limits usable systems
  - ensure scheduler sub-process terminates
  - fix cancellation procedure (executor modules)
  - fix rankfile / hostfile checks
  - fix `task['proc']` handling (in `Popen._check_running()`)
  - update py-versions for `ubuntu-latest`
  - update references to RP classes
  - update tutorials and docs


--------------------------------------------------------------------------------
### 1.90.0 Release                                                    2024-12-16

  - `glob` does not understand POSIX globs :-(
  - add `DOWNLOAD` staging action
  - add resource configuration for `epcc.archer2`
  - improve process watching
  - ensure cancel arrives at executor
  - external service support
  - fix delta config
  - fix example
  - fix io redirection for service tasks
  - generously increase proxy timeout
  - remove MacOS support
  - support non-ve mode
  - only service rank 0 provides info string
  - run examples in PR CI
  - safer srun termination
  - set default `scattered=False`
  - support multi-rank services
  - support ranks per node
  - target state is interpreted *after* output staging
  - track output files
  - updated action versions
  - use python to fetch URLs
  - we expect task metadata to be a dict - enforce and document


--------------------------------------------------------------------------------
### 1.83.0 Release                                                    2024-10-15

  - support numa aware app level scheduling
  - apply cb lock to pilot updates
  - fix task cancellation
  - initial implementation of task priorities
  - remove docker folder and link the tutorial repo in the README
  - rename artifacts
  - run example ci tests separately


--------------------------------------------------------------------------------
### 1.82.0 Release                                                    2024-10-01

  - always log full version also
  - ensure state setting
  - updated docs and configs for Delta (NCSA)


--------------------------------------------------------------------------------
### 1.81.0 Release                                                    2024-09-03

  - support for application level scheduling
  - apply LM blacklist
  - avoid uneccessary repr implementations
  - ensure cancellation request is forwarded to agent and scheduler
  - ensure full task info dict


--------------------------------------------------------------------------------
### 1.63.0 Release                                                    2024-08-20

  - add missing file
  - ensure that stdout/stderr strings are utf8
  - test temp files moved to /tmp/


--------------------------------------------------------------------------------
### 1.62.0 Release                                                    2024-07-01

  - add laplace config
  - update Rivanna config
  - fix node indexing for analytics (RA)


--------------------------------------------------------------------------------
### 1.61.0 Release                                                    2024-07-01

  - add virtenv mode warning
  - add SRUN option not to kill all ranks for non-mpi task if a single rank fails
  - add scheduler capable to reconfigure tasks requirements
  - better logging
  - change cfg src for agent info
  - configurable partitions
  - ensure proper resource info for RA
  - expose hook for agent config
  - fix node count
  - fix submission race
  - fix LM SRUN tests
  - fixes in `_start_service_ep`
  - longer timeouts for component startup
  - parallel flux init
  - remove deep copies to speed up handling of large numbers of tasks
  - support plain shell as named_env
  - update raptor_mpi.ipynb
  - update resource config for Frontier / Flux
  - update resource configuration for Polaris
  - adjust PSI/J launcher to use provided batch options/constraints


--------------------------------------------------------------------------------
### 1.60.0 Release                                                    2024-05-10

  - remove rct dist staging
  - added resource configuration for Flux on Frontier
  - cleanup defaults for virtenv setup
  - fix pre_exec handling, srun detection
  - fix codecov (using `CODECOV_TOKEN`)
  - iteration on event docs
  - make advance calls more uniform
  - re-enable event checking for sessions
  - set version requirement for RCT stack
  - sync with RU
  - updated tutorial `Configuration`
  - use srun for flux startup


--------------------------------------------------------------------------------
### 1.52.1 Hotfix Release                                             2024-04-18

  - more fixes for setuptools upgrade
  - remove sdist staging
  - simplify bootstrapper


--------------------------------------------------------------------------------
### 1.52.0 Release                                                    2024-04-15

  - fix for setuptools upgrade
  - added resource configuration for Flux on Frontier
  - use srun for flux startup


--------------------------------------------------------------------------------
### 1.49.2 Hotfix Release                                             2024-04-10

  - fix #3162: missing advance on failed agent staging, create target dir


--------------------------------------------------------------------------------
### 1.49.0 Release                                                    2024-04-05

  - MPIRUN/SRUN documentation update
  - inherit environment for local executor


--------------------------------------------------------------------------------
### 1.48.0 Release                                                    2024-03-24

  - remove SAGA as hard dependency
  - add `named_env` check to TD verfication
  - add dragon readme
  - add link for a monitoring page for Polaris
  - add supported platform Aurora (ALCF)
  - add SRUN to Bridges2 config and make it default
  - compensate job env isolation for tasks
  - disable env inheritance in PSIJ launcher
  - ensure string type on stdout/stderr
  - fix a pilot cancellation race
  - fix external proxy handling
  - fix non-integer gpu count for flux
  - fix staging scheme expansion
  - fix state reporting from psij
  - fix tarball staging for session
  - refactoring moves launch and exec script creation to executor base
  - removed Theta placeholder (Theta is decommissioned)
  - set original env as a base for LM env
  - update flake8, remove unneeded dependency
  - update ANL config file (added Aurora and removed obsolete platforms)
  - update doc page for Summit (rollback using conda env)
  - update resource configs for Summit
  - update table for supported platforms
  - use physical cores only (no threads) for Aurora for now


--------------------------------------------------------------------------------
### 1.47.0 Release                                                    2024-02-08

  - binder tutorial
  - expand / fix documentation and README, update policies
  - docs for psij deployment
  - raptor example now terminates on worker failures
  - fix raptor worker registration
  - fix flux startup
  - fix type for `LFS_SIZE_PER_NODE`
  - update HB config parameters for raptor


--------------------------------------------------------------------------------
### 1.46.2 Hotfix-Release                                             2024-02-02

  - fix detection of failed tasks


--------------------------------------------------------------------------------
### 1.46.1 Hotfix-Release                                             2024-01-17

  - fix type for `LFS_SIZE_PER_NODE`


--------------------------------------------------------------------------------
### 1.46.0 Release                                                    2024-01-11

  - pypi fix
  - configurabe raptor hb timeouts


--------------------------------------------------------------------------------
### 1.43.0 Release                                                    2024-01-10

  - add bragg prediction example
  - add initial agent scheduler documentation
  - add JSRUN_ERF setup for Summit's config
  - add mechanism to determine batch/non-batch RP starting
  - collect task PIDs through launch- and exec-scripts
  - ensure mpi4py for raptor example
  - fix ERF creation for JSRUN LM (and updated tests accordingly)
  - fix Popen test
  - fix `Task._update` method (`description` attribute)
  - fix `_get_exec` in Popen (based on provided comments)
  - fix parsing PIDs procedure (based on provided comments)
  - fix profiling in Popen (based on provided comments)
  - fix resource manager handling in `get_resource_config`
  - fix tasks handling in `prof_utils`
  - fix test for launch-/exec-scripts
  - follow-up on comments
  - forward after scheduling
  - keep `pids` dict empty if there is no ranks provided
  - moved collecting EXEC_PID into exec-script
  - preserve process id for tasks with `executable` mode
  - switch raptor to use the agent ve
  - update `metadata` within task description


--------------------------------------------------------------------------------
### 1.42.0 Release                                                    2023-12-04
  
  - AgentComponent forwards all state notifications
  - document event locations
  - MPI tutorial for RAPTOR
  - add mpi4py to the ci requirements
  - add `bulk_size` for the executing queue (for sub-agents)
  - add option `--ppn` for `PALS` flavor in MPIEXEC LM
  - amarel cfg
  - current version requires RU v1.43
  - fix Profiling tutorial (fails when executed outside from its directory)
  - collect service related data in registry
  - fix multi pilot example
  - move agent config generation to session init
  - remove obsolete Worker class
  - remove MongoDB module load from the Perlmutter config
  - remove mpi4py from doc requirements
  - save sub-agent config into Registry
  - sub-agents are no daemons anymore
  - update documentation for Polaris (GPUs assignment)
  - update launcher for `Agent_N`
  - update sub-agent config (in sync with the agent default config)
  - update "Describing Tasks" tutorial
  - use RMInfo `details` for LM options


--------------------------------------------------------------------------------
### 1.41.0 Release                                                    2023-10-17

  - fix RTD
  - replace MongoDB with ZMQ messaging
  - adapt resource config for `ccs.mahti` to the new structure
  - add description about input staging data
  - add method to track startup file with service URL (special case - SOMA)
  - add package `mpich` into CU and docs dependencies
  - add resource_description class
  - check agent sandbox existence
  - clean RPC handling
  - clean raptor RPC
  - deprecated `python.system_packages`
  - enable testing of all notebooks
  - enable tests for all devel-branches
  - fix heartbeat management
  - fix LM config initialization
  - fix RM LSF for Lassen (+ add platform config)
  - fix Session close options
  - fix TMGR Staging Input
  - fix `pilot_state` in bootstrapping
  - fix `task_pre_exec` configurable parameter for Popen
  - fix bootstrapping for sub-agents
  - keep pilot RPCs local
  - raptor worker: one profile per rank
  - let raptor use registry
  - shield agains missing mpi
  - sub-schema for `schemas`
  - switch to registry configs instead of config files
  - update testes
  - update handling of the service startup process
  - upload session when testing notebooks
  - use hb msg class type
  - version RP devel/nodb temporary


--------------------------------------------------------------------------------
### 1.37.0 Release                                                    2023-09-23

  - fix `default_remote_workdir` for `csc.mahti` platform
  - add README to description for pypi
  - link config tutorial
  - add raptor to API docs
  - add MPI flavor `MPI_FLAVOR_PALS`
  - add cpu-binding for LM MPIEXEC with the `MPI_FLAVOR_PALS` flavor
  - clean up Polaris config
  - fix raptor master hb_freq and hb_timeout
  - fix test for MPIRUN LM
  - fix tests for MPIEXEC LM
  - add csc.mahti resource config
  - add slurm inspection test


--------------------------------------------------------------------------------
### 1.36.0 Release                                                    2023-08-01

  - added pre-defined `pre_exec` for Summit (preserve `LD_LIBRARY_PATH` from LM)
  - fixed GPU discovery from SLURM env variables
  - increase raptor's heartbeat time
  

--------------------------------------------------------------------------------
### 1.35.0 Release                                                    2023-07-11

  - Improve links to resource definitions.
  - Improve typing in Session.get_pilot_managers
  - Provide a target for Sphinx `:py:mod:` role.
  - Un-hide "Utilities and helpers" section in API reference.
  - Use a universal and unique identifier for registered callbacks.
  - added option `--exact` for Rivanna (SRun LM)
  - fixes tests for PRs from forks (#2969)


--------------------------------------------------------------------------------
### 1.34.0 Release                                                    2023-06-22

  - major documentation overhaul
  - Fixes ticket #1577
  - Fixes ticket #2553
  - added tests for PilotManager methods (`cancel_pilots`, `kill_pilots`)
  - fixed configuration for Perlmutter
  - fixed env dumping for RP Agent
  - move timeout into `kill_pilots` method to delay forced termination
  - re-introduce a `use_mpi` flag


--------------------------------------------------------------------------------
### 1.33.0 Release                                                    2023-04-25

  - add a resource definition for rivanna at UVa.
  - add documentation for missing properties
  - add an exception for RAPTOR workers regarding GPU sharing
  - add an exception in case GPU sharing is used in SRun or MPIRun LMs
  - add configuration discovery for `gpus_per_node` (Slurm)
  - add `PMI_ID` env variable (related to Hydra)
  - add rank env variable for MPIExec LM
  - add resource config for Frontier@OLCF
  - add service task description verification
  - add interactive config to UVA
  - add raptor tasks to the API doc
  - add rank documentation
  - allow access to full node memory by default
  - changed type for `task['resources']`, let RADICAL-Analytics to
    handle it
  - changed type of `gpus_per_rank` attribute in `TaskDescription` (from
    `int` to `float`)
  - enforce correct task mode for raptor master/workers
  - ensure result_cb for executable tasks
  - ensure `session._get_task_sandbox` for raptor tasks
  - ensure that `wait_workers` raises RuntimeError during stop
  - ensure worker termination on raptor shutdown
  - fix CUDA env variable(s) setup for `pre_exec` (in POPEN executor)
  - fix `gpu_map` in Scheduler and its usage
  - fix ranks calculation
  - fix slots estimation process
  - fix tasks binding (e.g., bind task to a certain number of cores)
  - fix the process of requesting a correct number of cores/gpus (in
    case of blocked cores/gpus)
  - Fix path of task sandbox path
  - fix wait_workers
  - google style docstrings.
  - use parameter `new_session_per_task` within resource description to
    control input parameter `start_new_session` in `subprocess.Popen`
  - keep virtualenv as fallback if venv is missing
  - let SRun LM to get info about GPUs from configured slots
  - make slot dumps dependent on debug level
  - master rpc handles stop request
  - move from custom virtualenv version to `venv` module
  - MPI worker sync
  - Reading resources from created task description
  - reconcile different worker submission paths
  - recover bootstrap_0\_stop event
  - recover task description dump for raptor
  - removed codecov from test requirements (codecov is represented by
    GitHub actions)
  - removed `gpus_per_node` - let SAGA handle GPUs
  - removed obsolete configs (FUNCS leftover)
  - re-order worker initialization steps, time out on registration
  - support sandboxes for raptor tasks
  - sync JSRun LM options according to defined slots
  - update JSRun LM according to GPU sharing
  - update slots estimation and `core/gpu_map` creation
  - worker state update cb


## Past

Use past releases to reproduce an earlier experiments.

--------------------------------------------------------------------------------
### 1.21.0 Release                                                    2023-02-01

  - add worker rank heartbeats to raptor
  - ensure descr defaults for raptor worker submission
  - move `blocked_cores/gpus` under `system_architecture` in resource
  config
  - fix `blocked_cores/gpus` parameters in configs for ACCESS and ORNL
  resources
  - fix core-option in JSRun LM
  - fix inconsistency in launching order if some LMs failed to be
  created
  - fix thread-safety of PilotManager staging operations.
  - add ANL's polaris and polaris_interactive support
  - refactor raptor dispatchers to worker base class


--------------------------------------------------------------------------------
### 1.20.1 Hotfix Release                                             2023-01-07

  - fix task cancellation call


--------------------------------------------------------------------------------
### 1.20.0 Release                                                    2022-12-16

  - interactive amarel cfg
  - add docstring for run_task, remove sort
  - add option `-r` (number of RS per node) is case of GPU tasks
  - add `TaskDescription` attribute `pre_exec_sync`
  - add test for `Master.wait`
  - add test for tasks cancelling
  - add test for TMGR StagingIn
  - add comment for config addition Fixes #2089
  - add TASK_BULK_MKDIR_THRESHOLD as configurable Fixes #2089
  - agent does not need to pull failed tasks
  - bump python test env to 3.7
  - cleanup error reporting
  - document attributes as `attr`, not `data`.
  - extended tests for RM PBSPro
  - fix `allocated_cores/gpus` in PMGR Launching
  - fix commands per rank (either a single string command or list of
  commands)
  - fix JSRun test
  - fix nodes indexing (`node_id`)
  - fix option `-b` (`--bind`)
  - fix setup procedure for agent staging test(s)
  - fix executor test
  - fix task cancelation if task is waiting in the scheduler wait queue
  - fix Sphinx syntax.
  - fix worker state statistics
  - implement task timeout for popen executor
  - refactor popen task cancellation
  - removed `pre_rank` and `post_rank` from Popen executor
  - rename XSEDE to ACCESS #2676
  - reorder env setup per rank (by RP) and consider (enforce) CPU/GPU
  types
  - reorganized task/rank-execution processes and synced that with
  launch processes
  - support schema aliases in resource configs
  - task attribute `slots` is not required in an executor
  - unify raptor and non-raptor prof traces
  - update amarel cfg
  - update RM Fork
  - update RM PBSPro
  - update SRun option `cpus-per-task` - set the option if
  `cpu_threads > 0`
  - update test for PMGR Launching
  - update test for Popen (for pre/post_rank transformation)
  - update test for RM Fork
  - update test for JSRun (w/o ERF)
  - update test for RM PBSPro
  - update profile events for raptor tasks
  - interactive amarel cfg


--------------------------------------------------------------------------------
### 1.18.1 Hotfix Release                                             2022-11-01

  - fix Amarel configuration


--------------------------------------------------------------------------------
### 1.18.0 Release                                                    2022-10-11

  - move raptor profiles and logfiles into sandboxes\
  - consistent use of task modes\
  - derive etypes from task modes
  - clarify and troubleshoot raptor.py example
  - docstring update
  - make sure we issue a `bootstrap_0_stop` event
  - raptor tasks now create `rank_start/ranks_stop` events
  - reporte allocated resources for RA
  - set MPIRun as default LM for Summit
  - task manager cancel wont block: fixes #2336
  - update task description (focus on `ranks`)


--------------------------------------------------------------------------------
### 1.17.0 Release                                                    2022-09-15

  - add `docker compose` recipe.
  - add option `-gpu` for IBM Spectrum MPI
  - add comet resource config
  - add doc of env variable
  - add interactive schema to frontera config
  - add rcfg inspection utilities
  - also tarball log files, simplify code
  - clarify semantics on `file` and `pwd` schemas
  - document programmatical inspection resource definitions
  - ensure RADICAL_SMT setting, document for end user
  - fixed session cache (resolved `cachedir`)
  - fix ornl resource sbox and summit interactive mode
  - fix session test cleanup
  - keep Spock's resource config in sync with Crusher's config
  - make pilot launch and bootstrap CWD-independent
  - make staging schemas consistent for pilot and task staging
  - only use major and minor version for `prep_env` spec version
  - pilot profiles and logfiles are now transferred as tarball #2663
  - fix scheduler termination
  - remove deprecated FUNCS executor
  - support RP within interactive jobs
  - simple attempt on api level reconnect
  - stage_in.target fix for absolute path Fixes #2590
  - update resource config for Crusher@ORNL
  - use current working tree for docker rp source.


--------------------------------------------------------------------------------
### 1.16.0 Release                                                    2022-08-15

  - add check for exception message
  - add test for `Agent_0`
  - fix `cpu_threads` for special tasks (service, sub-agent)
  - fix `task['resources']` value
  - fix uid generation for components (use shared file for counters)
  - fix master task tmgr
  - fix raptor tests
  - fix rp serializer unittest
  - fix sub_agent keyerror
  - keep agent's config with sub-agents in sync with default one
  - remove confusion of task attribute names (slots vs. resources)
  - set default values for agent and service tasks descriptions
  - set env variable (`RP_PILOT_SANDBOX`) for agent and service tasks
  launchers
  - update exec profile events
  - update headers for mpirun- and mpiexec-modules
  - update LM env setup for `MPIRun` and `MPIExec` special case
  (MPT=true)
  - update LM IBRun
  - update mpi-info extraction


--------------------------------------------------------------------------------
### 1.15.1 Hotfix Release                                             2022-07-04

  - fix syntactic error in env prep script


--------------------------------------------------------------------------------
### 1.15.0 Release                                                    2022-07-04

  - added tests for PRTE LM
  - added tests for `rank_cmd` (IBRun and SRun LMs)
  - adding TMGR stats
  - adding xsede.expanse to the resource config
  - always interprete prep_env version request
  - anaconda support for prepare_env
  - Checking input staging exists before tar-ing Fixes #2483
  - ensure pip in venv mode
  - fixed `_rm_info` in IBRun LM
  - fixed status callback for SAGA Launcher
  - fixed type in `ornl.summit_prte` config
  - fix Ibrun set rank env
  - fix raptor env vals
  - use os.path to check if file exists Fixes #2483
  - remove node names duplication in SRun LM command
  - hide node-count from saga job description
  - 'state_history' is no longer supported
  - support existing VEs for `prepare_env`
  - updated installation of dependencies in bootstrapper
  - updated PRTE LM setup and config (including new release of PRRTE on
  Summit)
  - updating PMGR/AGENT stats - see #2401


--------------------------------------------------------------------------------
### 1.14.0 Release                                                    2022-04-13

  - support for MPI function tasks
  - support different RAPTOR workers
  - simplify / unify task and function descriptions
  - refactor resource aquisition
  - pilot submission via PSIJ or SAGA
  - added resource config for Crusher@OLCF/ORNL
  - support for execution of serialized function
  - pilot size can now be specified in number of nodes
  - support for PARSL integration
  - improved SMT handling
  - fixed resource configuration for `jsrun`
  - fix argument escapes
  - raptor consistently reports exceptions now


--------------------------------------------------------------------------------
### 1.13.0 Release                                                    2022-03-21

  - fix slurm nodefile/nodelist
  - clean temporary setup files
  - fixed test for LM `Srun`
  - local execution needs to check FORK first
  - fix Bridges-2 resource config


--------------------------------------------------------------------------------
### 1.12.0 Release                                                    2022-02-28

  - fix callback unregistration
  - fix capturing of task exit code
  - fix srun version command
  - fix metric setup / lookup in tmgr
  - get backfilling scheduler back in sync
  - re-introduced LM to handle `aprun`
  - Remove task log and the state_history
  - ru.Description -\> ru.TypedDict
  - set LM's initial env with activated VE
  - updated LSF handling cores indexing for LM JSRun
  - use proper shell quoting
  - use ru.TypedDict for Munch, fix tests


--------------------------------------------------------------------------------
### 1.11.2 Hotfix Release                                             2022-01-21

  - for non-mpi tasks, ensure that `$RP_RANK` is set to `0`


--------------------------------------------------------------------------------
### 1.11.0 Release                                                    2022-01-19

  - improve environment isolation for tasks and RCT components
  - add test for LM Srun
  - add resource manager instance to Executor base class
  - add test for blocked cores and gpus parameters (RM base)
  - add unittest to test LM base class initialization from Registry
  - add raptor test
  - add prepare_env example
  - add raptor request and result cb registration
  - avoid shebang use during bootstrap, pip sometimes screws it up
  - detect slurm version and use node file/list
  - enable nvme on summit
  - ensure correct out/err file paths
  - extended GPU handling
  - fix configs to be aligned with env isolation setup
  - fix LM PRTE rank setup command
  - fix `cfg.task_environment` handling
  - simplify BS env setup
  - forward resource reqs for raptor tasks
  - iteration on flux executor integration
  - limit pymongo version
  - provision radical-gtod
  - reconcile named env with env isolation
  - support Spock
  - support ALCF/JLSE Arcticus and Iris testbeds
  - fix staging behavior under `stage_on_error`
  - removed dead code


--------------------------------------------------------------------------------
### 1.10.2 Hotfix Release                                             2021-12-14

  - constrain mongodb version dependency


--------------------------------------------------------------------------------
### 1.10.0 Release                                                    2021-11-22

  - Add fallback for ssh tunnel on ifconfig-less nodes
  - cleanup old resources
  - removed OSG leftovers
  - updating test cases
  - fix recursive flag


--------------------------------------------------------------------------------
### 1.9.2 Hotfix Release                                              2021-10-27

  - fix shell escaping for task arguments


--------------------------------------------------------------------------------
### 1.9.0 Release                                                     2021-10-18

  - amarel cfg


--------------------------------------------------------------------------------
### 1.8.0 Release                                                     2021-09-23

  - fixed pilot staging for input directories
  - clean up configs
  - disabled `os.setsid` in `Popen` executor/spawner (in
  `subprocess.Popen`)
  - refreshed module list for Summit
  - return virtenv setup parameters
  - Support for :py:mod:`radical.pilot.X` links. (@eirrgang)
  - use local virtual env (either venv or conda) for Summit


--------------------------------------------------------------------------------
### 1.6.8 Hotfix Release                                              2021-08-24

  - adapt flux integration to changes in flux event model
  - fix a merge problem on flux termination handling


--------------------------------------------------------------------------------
### 1.6.7 Release                                                     2021-07-09

  - artifact upload for RA integration test
  - encapsulate kwargs handling for Session.close().
  - ensure state updates
  - fail tasks which can never be scheduled
  - fixed jsrun resource_set_file to use `cpu_index_using: logical`
  - separate cpu/gpu utilization
  - fix error handling in data stager
  - use methods from the new module `host` within RU (\>=1.6.7)


--------------------------------------------------------------------------------
### 1.6.6 Release                                                     2021-05-18

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


--------------------------------------------------------------------------------
### 1.6.5 Release                                                     2021-04-14

  - added flag `exclusive` for tags (in task description, default
  `False`)
  - Adding Bridges2 and Comet
  - always specifu GPU number on srun
  - apply RP+\* env vars to raptor tasks
  - avoid a termination race
  - Summit LFS config and JSRUN integration tests
  - gh workflows and badges
  - ensure that RU lock names are unique
  - fixed env creation command and updated env setup check processes
  - fixed launch command for PRTE2 LM
  - fix missing event updates
  - fix ve isolation for prep_env
  - keep track of tagged nodes (no nodes overlapping between different
  tags)
  - ensure conda activate works
  - allow output staging on failed tasks
  - python 2 -\> 3 fix for shebangs
  - remove support for add_resource_config
  - Stampede2 migrates to work2 filesystem
  - update setup module (use `python3`)


--------------------------------------------------------------------------------
### 1.6.3 Hotfix Release                                              2021-04-03

  - fix uid assignment for managers


--------------------------------------------------------------------------------
### 1.6.2 Hotfix Release                                              2021-03-26

  - switch to pep-440 for sdist and wheel versioning, to keep pip happy


--------------------------------------------------------------------------------
### 1.6.1 Release                                                     2021-03-09

  - support for Andes@ORNL, obsolete Rhea@ORNL
  - add_pilot() also accepts pilot dict
  - fixed conda activation for PRTE2 config (Summit@ORNL)
  - fixed partitions handling in LSF_SUMMIT RM
  - reorganized DVM start process (prte2)
  - conf fixes for comet
  - updated events for PRTE2 LM
  - integration test for Bridges2
  - prepare partitioning


--------------------------------------------------------------------------------
### 1.6.0 Release                                                     2021-02-13

  - rename ComputeUnit -\> Task
  - rename ComputeUnitDescription -\> TaskDescription
  - rename ComputePilot -\> Pilot
  - rename ComputePilotDescription -\> PilotDescription
  - rename UnitManager -\> TaskManager
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


--------------------------------------------------------------------------------
### 1.5.12 Release                                                    2021-02-02

  - multi-dvm support
  - cleanup of raptor
  - fix for bootstrap profiling
  - fix help string in bin/radical-pilot-create-static-ve
  - forward compatibility for tags
  - fix data stager for multi-pilot case
  - parametric integration tests
  - scattered fixes for raptor and sub-agent profiling
  - support new resource utilization plots


--------------------------------------------------------------------------------
### 1.5.11 Release                                                    2021-01-19

  - cleanup pypi tarball


--------------------------------------------------------------------------------
### 1.5.10 Release                                                    2021-01-18

  - gpu related fixes (summit)
  - avoid a race condition during termination
  - fix bootstrapper timestamps
  - fixed traverse config
  - fix nod counting for FORK rm
  - fix staging context
  - move staging ops into separate worker
  - use C locale in bootstrapper


--------------------------------------------------------------------------------
### 1.5.8 Release                                                     2020-12-09

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


--------------------------------------------------------------------------------
### 1.5.7 Release                                                     2020-10-30

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


--------------------------------------------------------------------------------
### 1.5.4 Release                                                     2020-10-01

  - fix jsrun GPU mapping


--------------------------------------------------------------------------------
### 1.5.4 Release                                                     2020-09-14

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


--------------------------------------------------------------------------------
### 1.5.2 Hotfix Release                                              2020-08-11

  - add/fix RA prof metrics
  - clean dependencies
  - fix RS file system cache


--------------------------------------------------------------------------------
### 1.5.1 Hotfix Release                                              2020-08-05

  - added config parameter for MongoDB tunneling
  - applied exception chaining
  - filtering for login/batch nodes that should not be considered (LSF
  RM)
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
  - RADICAL_BASE_DIR -\> RADICAL_BASE
  - remove private TMPDIR export - this fixes #2158
  - Remove SKIP_FAILED (unused)
  - support for custom batch job names
  - updated cuda hook for JSRUN LM
  - updated license file
  - updated readme
  - updated version requirement for python (min is 3.6)


--------------------------------------------------------------------------------
### 1.4.1 Hotfix Release                                              2020-06-09

  - fix tmpdir mosconfiguration for summit / prrte


--------------------------------------------------------------------------------
### 1.4.0 Release                                                     2020-05-12

  - merge #2122: fixed `n_nodes` for the case when `slots` are set
  - merge #2123: fix #2121
  - merge #2124: fixed conda-env path definition
  - merge #2127: bootstrap env fix
  - merge #2133, #2138: IBRun fixes
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


--------------------------------------------------------------------------------
### 1.3.0 Release                                                     2020-04-10

  - task overlay + docs
  - iteration on srun placement
  - add env support to srun
  - theta config
  - clean up launcher termination guard against lower level termination
  errors
  - cobalt rm
  - optional output stager
  - revive ibrun support
  - switch comet FS


--------------------------------------------------------------------------------
### 1.2.1 Hotfix Release                                              2020-02-11

  - scattered fixes cfor summit


--------------------------------------------------------------------------------
### 1.2.0 Release                                                    2020-02-11

  - support for bulk callbacks
  - fixed package paths for launch methods
  (radical.pilot.agent.launch_method)
  - updated documentation references
  - raise minimum Python version to 3.6
  - local submit configuration for Frontera
  - switch frontera to default agent cfg
  - fix cray agent config
  - fix issue #2075 part 2


--------------------------------------------------------------------------------
### 1.1.1 Hotfix Release                                              2020-02-11

  - fix dependency version for radical.utils


--------------------------------------------------------------------------------
### 1.1 Release                                                       2020-02-11

  - code cleanup


--------------------------------------------------------------------------------
### 1.0.0 Release                                                     2019-12-24

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
  - support for Frontera
  - lockfree scheduler base class
  - switch to RU ZMQ layer


--------------------------------------------------------------------------------
### 0.90.1 Release                                                    2019-10-12

  - port pubsub hotfix


--------------------------------------------------------------------------------
### 0.90.0 Release                                                    2019-10-07

  - transition to Python3


--------------------------------------------------------------------------------
### 0.73.1 Release                                                    2019-10-07

  - Stampede-2 support


--------------------------------------------------------------------------------
### 0.72.2 Hotfix Release                                             2019-09-30

  - fix sandbox setting on absolute paths


--------------------------------------------------------------------------------
### 0.72.0 Release                                                    2019-09-11

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


--------------------------------------------------------------------------------
### 0.70.3 Hotfix Release                                             2019-08-02

  - fix example and summit configuration


--------------------------------------------------------------------------------
### 0.70.2 Hotfix Release                                             2019-07-31

  - fix static ve creation for Tiger (Princeton)


--------------------------------------------------------------------------------
### 0.70.1 Hotfix Release                                             2019-07-30

  - fix configuration for Tiger (Princeton)


--------------------------------------------------------------------------------
### 0.70.0 Release                                                    2019-07-07

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


--------------------------------------------------------------------------------
### 0.63.0 Release                                                    2019-06-25

  - support for summit (experimental, jsrun + ERF)
  - PRRTE support (experimental, summit only)
  - many changes to the test setup (pytest, pylint, flake8, coverage,
  travis)
  - support for Tiger (adds SRUN launch method)
  - support NOOP scheduler
  - support application level communication
  - support ordered scheduling of tasks
  - partial code cleanup (coding guidelines)
  - simplifying MPI base launch methods
  - support for resource specific SMT settings
  - resource specific ranges of cores/threads can now be blocked from
  use
  - ORTE support is doscontinued
  - fixes in hombre scheduler
  - improvements on GPU support
  - fix in continuous scheduler which caused underutilization on
  heterogeneous tasks
  - fixed: #1758, #1764, #1792, #1790, #1827, #187


--------------------------------------------------------------------------------
### 0.62.0 Release                                                    2019-06-08

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


--------------------------------------------------------------------------------
### 0.61.0 Release                                                    2019-05-07

  - add travis support, test cleanup
  - ensure safe bootstrapper termination on faulty installs
  - push node_list to mongodb for analytics
  - fix default dburl
  - fix imports in tests
  - remove deprecated special case in bootstrapper


--------------------------------------------------------------------------------
### 0.60.1 Hotfix                                                     2019-04-12

  - work around a pip install problem


--------------------------------------------------------------------------------
### 0.60.0 Release                                                    2019-04-10

  - add issue template
  - rename RP_PILOT_SBOX to RP_PILOT_STAGING and expose to tasks
  - fix bridges default partition (#1816)
  - fix #1826
  - fix off-by-one error on task state check
  - ignore failing DB disconnect
  - follow rename of saga-python to radical.saga


--------------------------------------------------------------------------------
### 0.50.23 Release                                                   2019-03-20

  - hotfix: use popen spawner for localhost


--------------------------------------------------------------------------------
### 0.50.22 Release                                                   2019-02-11

  - another fix LSF var expansion


--------------------------------------------------------------------------------
### 0.50.21 Release                                                   2018-12-19

  - fix LSF var expansion


--------------------------------------------------------------------------------
### 0.50.20 Release                                                   2018-11-25

  - fix Titan OMPI installation
  - support metdata for tasks
  - fix git error detection during setup


--------------------------------------------------------------------------------
### 0.50.19 Release                                                   2018-11-15

  - ensure profile fetching on empty tarballs


--------------------------------------------------------------------------------
### 0.50.18 Release                                                   2018-11-13

  - support for data locality aware scheduling


--------------------------------------------------------------------------------
### 0.50.17 Release                                                   2018-10-31

  - improve event documentation
  - support Task level metadata


--------------------------------------------------------------------------------
### 0.50.16 Release                                                   2018-10-26

  - add new shell spawner as popen replacement


--------------------------------------------------------------------------------
### 0.50.15 Release                                                   2018-10-24

  - fix recursive pilot staging


--------------------------------------------------------------------------------
### 0.50.14 Release                                                   2018-10-24

  - add Cheyenne support - thanks Vivek!


--------------------------------------------------------------------------------
### 0.50.13 Release                                                   2018-10-16

  - survive if SAGA does not support job.name (#1744)


--------------------------------------------------------------------------------
### 0.50.12 Release                                                   2018-10-12

  - fix stacksize usage on BW


--------------------------------------------------------------------------------
### 0.50.11 Release                                                   2018-10-09

  - fix 'getting_started' example (no MPI)


--------------------------------------------------------------------------------
### 0.50.10 Release                                                   2018-09-29

  - ensure the correct code path in SAGA for Blue Waters


--------------------------------------------------------------------------------
### 0.50.9 Release                                                    2018-09-28

  - fix examples
  - fix issue #1715 (#1716)
  - remove Stampede's resource configs. issue #1711
  - supermic does not like `curl -1` (#1723)


--------------------------------------------------------------------------------
### 0.50.8 Release                                                    2018-08-03

  - make sure that CUD values are not None (#1688)
  - don't limit pymongo version anymore (#1687)


--------------------------------------------------------------------------------
### 0.50.7 Release                                                    2018-08-01

  - fix bwpy handling


--------------------------------------------------------------------------------
### 0.50.6 Release                                                    2018-07-31

  - fix curl tssl negotiation problem (#1683)


--------------------------------------------------------------------------------
### 0.50.5 Release                                                    2018-07-30

  - fix default values for process and thread types (#1681)
  - fix outdated links in ompi deploy script
  - fix/issue 1671 (#1680)
  - fix scheduler config checks (#1677)


--------------------------------------------------------------------------------
### 0.50.4 Release                                                    2018-07-13

  - set oversubscribe default to True


--------------------------------------------------------------------------------
### 0.50.3 Release                                                    2018-07-11

  - disable rcfg expnsion


--------------------------------------------------------------------------------
### 0.50.2 Release                                                    2018-07-08

  - fix relative tarball unpack paths


--------------------------------------------------------------------------------
### 0.50.1 Release                                                    2018-07-05

  - GPU support
  - many bug fixes


--------------------------------------------------------------------------------
### 0.47.14 Release                                                   2018-06-13

  - fix recursive output staging


--------------------------------------------------------------------------------
### 0.47.13 Release                                                   2018-06-02

  - catch up with RU log, rep and prof settings


--------------------------------------------------------------------------------
### 0.47.12 Release                                                   2018-05-19

  - ensure that tasks are started in their own process group, to ensure
  clean cancellation semantics.


--------------------------------------------------------------------------------
### 0.47.11 Release                                                   2018-05-08

  - fix schemas on BW (local orte, local aprun)


--------------------------------------------------------------------------------
### 0.47.10 Release                                                   2018-04-19

  - fix #1602


--------------------------------------------------------------------------------
### 0.47.9 Release                                                    2018-04-18

  - fix default scheduler for localhost


--------------------------------------------------------------------------------
### 0.47.8 Release                                                    2018-04-16

  - hotfix to catch up with pypi upgrade


--------------------------------------------------------------------------------
### 0.47.7 Release                                                    2018-04-15

  - bugfix related to radical.entk #255


--------------------------------------------------------------------------------
### 0.47.6 Release                                                    2018-04-12

  - bugfix related to #1590


--------------------------------------------------------------------------------
### 0.47.5 Release                                                    2018-04-12

  - make sure a dict object exists even on empty env settings (#1590)


--------------------------------------------------------------------------------
### 0.47.4 Release                                                    2018-03-20

  - fifo agent scheduler (#1537)
  - hombre agent scheduler (#1536)
  - Fix/issue 1466 (#1544)
  - Fix/issue 1501 (#1541)
  - switch to new OMPI deployment on titan (#1529)
  - add agent configuration doc (#1540)


--------------------------------------------------------------------------------
### 0.47.3 Release                                                    2018-03-20

  - add resource limit test
  - add tmp cheyenne config
  - api rendering proposal for partitions
  - fix bootstrap sequence (BW)
  - tighten bootstrap process, add documentation


--------------------------------------------------------------------------------
### 0.47.2 Release                                                    2018-02-28

  - fix issue 1538
  - fix issue 1554
  - expose profiler to LM hooks (#1522)
  - fix bin names (#1549)
  - fix event docs, add an event for symmetry (#1527)
  - name attribute has been changed to uid, fixes issue #1518
  - make use of flags consistent between RP and RS (#1547)
  - add support for recursive data staging (#1513. #1514) (JD, VB, GC)
  - change staging flags to integers (inherited from RS)
  - add support for bulk data transfer (#1512) (IP, SM)


--------------------------------------------------------------------------------
### 0.47 Release                                                      2017-11-19

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
  - centralize URL derivation for pilot job service endpoints, hops, and
  sandboxes
  - clarify use of namespace vs. full qualified URL in the context of RP
  file staging
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
  - publish resource_details (incl. lm_info) again
  - re-add a profile flag to advance()
  - remove old controllers
  - remove old files
  - remove uid clashes for sub-agent components and components in
  general
  - setup number of cores per node on stampede2
  - smaller default pilot size for supermic
  - switch to ibrun for comet_ssh
  - track task drops
  - use js hop for untar
  - using new process class
  - GPU/CPU pinning test is now complete, needs some env settings in the
  launchers


--------------------------------------------------------------------------------
### 0.46.2 Release                                                    2017-09-02

  - hotfix for #1426 - thanks Iannis!


--------------------------------------------------------------------------------
### 0.46.1 Release                                                    2017-08-23

  - hotfix for #1415


--------------------------------------------------------------------------------
### Version 0.46                                                      2017-08-11

  - TODO


--------------------------------------------------------------------------------
### 0.45.3 Release                                                    2017-05-09

  - Documentation update for the BW tutorial


--------------------------------------------------------------------------------
### 0.45.1 Release                                                    2017-03-05

  - NOTE: OSG and ORTE_LIB on titan are considered unsupported. You can
  enable those resources for experiments by setting the `enabled` keys
  in the respective config entries to `true`.
  - hotfix the configurations markers above


--------------------------------------------------------------------------------
### 0.45 Release                                                      2017-02-28

  - NOTE: OSG and ORTE_LIB on titan are considered unsupported. You can
  enable those resources for experiments by removing the comment
  markers from the respective resource configs.
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
  - fix state transition to UNSCHEDDULED to avoid repetition and invalid
  state ordering


--------------------------------------------------------------------------------
### 0.44.1 Release                                                    2016-11-01

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
  - when calling a task state callback, missed states also trigger
  callbacks


--------------------------------------------------------------------------------
### 0.43.1 Release                                                    2016-09-09

  - hotfix: fix netifaces to version 0.10.4 to avoid trouble on
  BlueWaters


--------------------------------------------------------------------------------
### 0.43 Release                                                      2016-09-08

  - Add aec_handover for orte.
  - add a local confiuration for bw
  - add early binding eample for osg
  - add greenfield config (only works for single-node runs at the
  moment)
  - add PYTHONPATH to the vars we reset for Task envs
  - allow overloading of agent config
  - fix #1071
  - fix synapse example
  - avoid profiling of empty state transitions
  - Check of YARN start-all script. Raising Runtime error in case of
  error.
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
  - SchedulerContinuous -\> AgentSchedulingComponent.
  - Take ccmrun out of bootstrap_2.
  - Tempfile is not a tempfile so requires explicit removal.
  - resolve #1001
  - Unbreak CCM.
  - use high water mark for ZMQ to avoid message drops on high loads


--------------------------------------------------------------------------------
### 0.42 Release                                                      2016-08-09

  - change examples to use 2 cores on localhost
  - Iterate documentation
  - Manual cherry pick fix for getip.


--------------------------------------------------------------------------------
### 0.41 Release                                                      2016-07-15

  - address some of error messages and type checks
  - add scheduler documentation simplify interpretation of BF
  oversubscription fix a log message
  - fix logging problem reported by Ming and Vivek
  - global default url, sync profile/logfile/db fetching tools
  - make staging path resilient against cwd changes
  - Switch SSH and ORTE for Comet
  - sync session cleanup tool with rpu
  - update allocation IDs


--------------------------------------------------------------------------------
### 0.40.4 Release                                                    2016-05-18

  - point release with more tutorial configurations


--------------------------------------------------------------------------------
### 0.40.3 Release                                                    2016-05-17

  - point release with tutorial configurations


--------------------------------------------------------------------------------
### 0.40.2 Release                                                    2016-05-13

  - hotfix to fix vnode parsing on archer


--------------------------------------------------------------------------------
### 0.40.1 Release                                                    2016-02-11

  - hotfix which makes sure agents don't report FAILED on cancel()


--------------------------------------------------------------------------------
### 0.40 Release                                                      2016-02-03

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


--------------------------------------------------------------------------------
### 0.38 Release                                                      2015-12-22

  - fix busy mongodb pull


--------------------------------------------------------------------------------
### 0.37.10 Release                                                   2015-10-20

  - config fix


--------------------------------------------------------------------------------
### 0.37.9 Release                                                    2015-10-20

  - Example fix


--------------------------------------------------------------------------------
### 0.37.8 Release                                                    2015-10-20

  - Allocation fix


--------------------------------------------------------------------------------
### 0.37.7 Release                                                    2015-10-20

  - Allocation fix


--------------------------------------------------------------------------------
### 0.37.6 Release                                                    2015-10-20

  - Documentation


--------------------------------------------------------------------------------
### 0.37.5 Release                                                    2015-10-19

  - timing fix to ensure task state ordering


--------------------------------------------------------------------------------
### 0.37.3 Release                                                    2015-10-19

  - small fixes, doc changes


--------------------------------------------------------------------------------
### 0.37.2 Release                                                    2015-10-18

  - fix example installation


--------------------------------------------------------------------------------
### 0.37.1 Release                                                    2015-10-18

  - update of documentation and examples
  - some small fixes on shutdown installation


--------------------------------------------------------------------------------
### 0.37 Release                                                      2015-10-15

  - change default spawner to POPEN
  - use hostlist to avoid mpirun\* limitations
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
  - make localhost (ForkLRMS) behave like a resource with an inifnite
  number of cores


--------------------------------------------------------------------------------
### 0.36 Release                                                      2015-10-08

(the release notes also cover some changes from 0.34 to 0.35)

  - simplify agent process tree, process naming
  - improve session and agent termination
  - several fixes and chages to the task state model (refer to
  documentation!)
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
  - several resource specific configuration fixes (mostly stampede,
  archer, bw)
  - backport stdout/stderr/log retrieval
  - better logging of clone/drops, better error handling for configs
  - fix, improve profiling of Task execution
  - make profile an object
  - use ZMQ pubsub and queues for agent/sub-agent communication
  - decouple launch methods from scheduler for most LMs NOTE: RUNJOB
  remains coupled!
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
  (incl. GlobusOnline)
  - work toward better OSG support
  - Use netifaces for ip address mangling.
  - Use ORTE from the 2.x branch.
  - remove Url class


--------------------------------------------------------------------------------
### 0.35.1 Release                                                    2015-09-29

  - hotfix to use popen on localhost


--------------------------------------------------------------------------------
### 0.35 Release                                                      2015-07-14

  - numerous bug fixes and support for new resources


--------------------------------------------------------------------------------
### 0.34 Release                                                      2015-07-14

  - Hotfix release for an installation issue


--------------------------------------------------------------------------------
### 0.33 Release                                                      2015-05-27

  - Hotfix release for off-by-one error (#621)


--------------------------------------------------------------------------------
### 0.32 Release                                                      2015-05-18

  - Hotfix release for MPIRUN_RSH on Stampede (#572).


--------------------------------------------------------------------------------
### 0.31 Release                                                      2015-04-30

  - version bump to trigger pypi release update


--------------------------------------------------------------------------------
### 0.30 Release                                                      2015-04-29

  - hotfix to handle broken pip/bash combo on archer


--------------------------------------------------------------------------------
### 0.29 Release                                                      2015-04-28

    - hotfix to handle stale ve locks


--------------------------------------------------------------------------------
### 0.28 Release                                                      2015-04-16

  - This release contains a very large set of commits, and covers a
  fundamental overhaul of the RP agent (amongst others). It also
  includes:
  - support for agent profiling
  - removes a number of state race conditions
  - support for new backends (ORTE, CCM)
  - fixes for other backends
  - revamp of the integration tests


--------------------------------------------------------------------------------
### 0.26 Release                                                      2015-04-08

    - hotfix to cope with API changing pymongo release


--------------------------------------------------------------------------------
### 0.25 Release                                                      2015-04-01

  - hotfix for a stampede configuration change


--------------------------------------------------------------------------------
### 0.24 Release                                                      2015-03-30

    - More support for URLs in StagingDirectives (#489).
    - Create parent directories of staged files.
    - Only process entries for Output FTW, fixes #490.
    - SuperMUC config change.
    - switch from bson to json for session dumps
    - fixes #451
    - update resources.rst
    - remove superfluous ```\n`{=tex}``
    - fix #438
    - add documentation on resource config changes, closes #421
    - .ssh/authorized_keys2 is deprecated since 2011
    - improved intra-node SSH FAQ item


--------------------------------------------------------------------------------
### 0.23 Release                                                      2014-12-13

  - fix #455


--------------------------------------------------------------------------------
### 0.22 Release                                                      2014-12-11

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
    - added configuration for BlueBiou (Thanks Jordane)
    - better support for json/bson/timestamp handling; cache mongodb data
      for stats, plots etc
    - localize numpy dependency
    - retire input_data and output_data
    - remove obsolete staging examples
    - address #410
    - fix another subtle state race


--------------------------------------------------------------------------------
### 0.21 Release                                                      2014-10-29

  - Documentation of MPI support
  - Documentation of data staging operations
  - correct handling of data transfer exceptions
  - fix handling of non-ascii data in task stdio
  - simplify switching of access schemas on pilot submission
  - disable pilot virtualenv for task execution
  - MPI support for DaVinci
  - performance optimizations on file transfers, task sandbox setup
  - fix ibrun tmp file problem on stampede


--------------------------------------------------------------------------------
### 0.19 Release                                              September 12. 2014

  - The Milestone 8 release (MS.8)
  - [Closed Tickets](https://github.com/radical-cybertools/radical.pilot/issues?q=is%3Aclosed+milestone%3AMS-8+)


--------------------------------------------------------------------------------
### 0.18 Release                                                   July 22. 2014

  - The Milestone 7 release (MS.7)
  - [Closed Tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=13&state=closed)


--------------------------------------------------------------------------------
### 0.17 Release                                                   June 18. 2014

  - Bugfix release - fixed file permissions et al. :/


--------------------------------------------------------------------------------
### 0.16 Release                                                   June 17. 2014

  - Bugfix release - fixed file permissions et al.


--------------------------------------------------------------------------------
### 0.15 Release                                                   June 12. 2014

  - Bugfix release
  - fixed distribution MANIFEST: [issues #174](https://github.com/radical-cybertools/radical.pilot/issues/174)


--------------------------------------------------------------------------------
### 0.14 Release                                                   June 11. 2014

  - [Closed tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=16&state=closed)

New Features

  - Experimental pilot-agent for Cray systems
  - New multi-core agent with MPI support
  - New ResourceConfig mechanism does not reuquire the user to add
  resource configurations explicitly. Resources can be configured
  programatically on API-level.

API Changes:

  - TaskDescription.working_dir_priv removed
  - Extended state model
  - resource_configurations parameter removed from PilotManager c\`tor


--------------------------------------------------------------------------------
### 0.13 Release                                                    May 19. 2014

  - ExTASY demo release
  - Support for project / allocation
  - Updated / simplified resource files
  - Refactored bootstrap mechnism


--------------------------------------------------------------------------------
### 0.12 Release                                                    May 09. 2014

  - Updated resource files
  - Updated state model
  - [Closed tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=12&state=closed)


--------------------------------------------------------------------------------
### 0.11 Release                                                   Apr. 29. 2014

  - Fixes error in state history reporting


--------------------------------------------------------------------------------
### 0.10 Release Apr.                                                   29. 2014

  - Support for state transition introspection via Task/Pilot
  state_history
  - Cleaned up an streamlined Input and Outpout file transfer workers
  - Support for interchangeable pilot agents
  - [Closed tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=11&state=closed)


--------------------------------------------------------------------------------
### 0.9 Release                                                    Apr. 16. 2014

  - Support for output file staging
  - Streamlines data model
  - More loosely coupled components connected via DB queues
  - [Closed tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=10&state=closed)


--------------------------------------------------------------------------------
### 0.8 Release                                                    Mar. 24. 2014

  - Renamed codebase from sagapilot to radical.pilot
  - Added explicit close() calls to PM, UM and Session.
  - [Closed tickets](https://github.com/radical-cybertools/radical.pilot/issues?milestone=9&state=closed)


--------------------------------------------------------------------------------
### 0.7 Release                                                    Feb. 25. 2014

  - Added support for callbacks
  - Added support for input file transfer !


--------------------------------------------------------------------------------
### 0.6 Release                                                    Feb. 24. 2014

  - BROKEN RELEASE


--------------------------------------------------------------------------------
### 0.5 Release                                                    Feb. 06. 2014

  - Tutorial 2 release (Github only)
  - Added support for multiprocessing worker
  - Support for Task stdout and stderr transfer via MongoDB GridFS


--------------------------------------------------------------------------------
### 0.4 Release

  - Tutorial 1 release (Github only)
  - Consistent naming (sagapilot instead of sinon)


--------------------------------------------------------------------------------
### 0.1.3 Release

  - Github only release
  - Added logging
  - Added security context handling


--------------------------------------------------------------------------------
### 0.1.2 Release

  - Github only release

--------------------------------------------------------------------------------

  -------------------------------------------------------------
  -------------------------------------------------------------
