
# pylint: disable=access-member-before-definition
#
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from .resource_config import Slot
from .utils.misc      import FastTypedDict


# task modes
TASK_EXECUTABLE  = 'task.executable'
TASK_SERVICE     = 'task.service'
TASK_FUNCTION    = 'task.function'
TASK_FUNC        = 'task.function'
TASK_METHOD      = 'task.method'
TASK_METH        = 'task.method'
TASK_EVAL        = 'task.eval'
TASK_EXEC        = 'task.exec'
TASK_PROC        = 'task.proc'
TASK_SHELL       = 'task.shell'
RAPTOR_MASTER    = 'raptor.master'
RAPTOR_WORKER    = 'raptor.worker'
AGENT_SERVICE    = 'agent.service'

# task description attributes
UID              = 'uid'
NAME             = 'name'
MODE             = 'mode'

# mode: TASK_EXECUTABLE
EXECUTABLE       = 'executable'
ARGUMENTS        = 'arguments'

# mode: TASK_SERVICE
EXECUTABLE       = 'executable'
ARGUMENTS        = 'arguments'
INFO_PATTERN     = 'info_pattern'

# mode: TASK_METHOD
METHOD           = 'method'
ARGS             = 'args'
KWARGS           = 'kwargs'

# mode: TASK_FUNC
FUNCTION         = 'function'
ARGS             = 'args'
KWARGS           = 'kwargs'

# mode: TASK_EXEC, TASK_EVAL
CODE             = 'code'

# mode: TASK_PROC, TASK_SHELL
COMMAND          = 'command'

# mode: RAPTOR_MASTER, RAPTOR_WORKER
SCHEDULER        = 'scheduler'                # deprecated for `raptor_id`
RAPTOR_ID        = 'raptor_id'
WORKER_CLASS     = 'worker_class'             # deprecated for raptor_class
RAPTOR_CLASS     = 'raptor_class'
WORKER_FILE      = 'worker_file'              # deprecated for raptor_file
RAPTOR_FILE      = 'raptor_file'

# environment
ENVIRONMENT      = 'environment'
NAMED_ENV        = 'named_env'
SANDBOX          = 'sandbox'

# resource requirements
USE_MPI          = 'use_mpi'                  # default `True if RANKS > 1`
RANKS            = 'ranks'                    # ranks
RANKS_PER_NODE   = 'ranks_per_node'           # ranks per node
CORES_PER_RANK   = 'cores_per_rank'           # cores per rank
GPUS_PER_RANK    = 'gpus_per_rank'            # gpus per rank
THREADING_TYPE   = 'threading_type'           # OpenMP?
GPU_TYPE         = 'gpu_type'                 # CUDA / ROCm?
LFS_PER_RANK     = 'lfs_per_rank'             # disk space per rank
MEM_PER_RANK     = 'mem_per_rank'             # memory per rank
PARTITION        = 'partition'                # partition to run on

# deprecated
CPU_PROCESSES    = 'cpu_processes'            # ranks
CPU_PROCESS_TYPE = 'cpu_process_type'         # n/a
CPU_THREADS      = 'cpu_threads'              # cores per rank
CPU_THREAD_TYPE  = 'cpu_thread_type'          # OpenMP?

GPU_PROCESSES    = 'gpu_processes'            # gpus per rank
GPU_PROCESS_TYPE = 'gpu_process_type'         # CUDA?
GPU_THREADS      = 'gpu_threads'              # part of gpu?
GPU_THREAD_TYPE  = 'gpu_thread_type'          # ?

LFS_PER_PROCESS  = 'lfs_per_process'          # disk space per rank
MEM_PER_PROCESS  = 'mem_per_process'          # memory per rank

# task setup
INPUT_STAGING    = 'input_staging'
OUTPUT_STAGING   = 'output_staging'
STAGE_ON_ERROR   = 'stage_on_error'
PRIORITY         = 'priority'
PRE_LAUNCH       = 'pre_launch'
PRE_EXEC         = 'pre_exec'
PRE_EXEC_SYNC    = 'pre_exec_sync'
POST_LAUNCH      = 'post_launch'
POST_EXEC        = 'post_exec'
TIMEOUT          = 'timeout'
STARTUP_TIMEOUT  = 'startup_timeout'
CLEANUP          = 'cleanup'
PILOT            = 'pilot'
SLOTS            = 'slots'
STDOUT           = 'stdout'
STDERR           = 'stderr'
RESTARTABLE      = 'restartable'
TAGS             = 'tags'
SERVICES         = 'services'
METADATA         = 'metadata'


# ------------------------------------------------------------------------------
#
class TaskDescription(FastTypedDict):
    """Describe the requirements and properties of a Task.

    A TaskDescription object describes the requirements and properties of a
    :class:`radical.pilot.Task` and is passed as a parameter to
    :func:`radical.pilot.TaskManager.submit_tasks` to instantiate and run a new
    task.

    Attributes:
        uid (str, optional): A unique ID for the task. This attribute
            is optional, a unique ID will be assigned by RP if the field is not
            set.

        name (str, optional): A descriptive name for the task. This
            attribute can be used to map individual tasks back to application
            level workloads.

        mode (str, optional): The execution mode to be used for
            this task. Default "executable". The following modes are accepted.

            - TASK_EXECUTABLE: the task is spawned as an external executable via
              a resource specific launch method (srun, aprun, mpiexec, etc).

              - required attributes: `executable`
              - related  attributes: `arguments`

            - TASK_SERVICE: exactly like TASK_EXECUTABLE, but the task is
              handled differently by the agent.  This mode is used to start
              service tasks whose connection endpoint is made available to other
              tasks.

              NOTE: the mode `TASK_SERVICE` is only used internally and should
              not be used by application developers.

              - required attributes: `executable`
              - related  attributes: `arguments`

            - TASK_FUNCTION: the task references a python function to be called.

              - required attributes: `function`
              - related  attributes: `args`
              - related  attributes: `kwargs`

            - TASK_METHOD: the task references a raptor worker method to be
              called.

              - required attributes: `method`
              - related  attributes: `args`
              - related  attributes: `kwargs`

            - TASK_EVAL: the task is a code snippet to be evaluated.

              - required attributes: `code`

            - TASK_EXEC: the task is a code snippet to be `exec`'ed.

              - required attributes: `code`

            - TASK_SHELL: the task is a shell command line to be run.

              - required attributes: `command`

            - TASK_PROC: the task is a single core process to be executed.

              - required attributes: `executable`
              - related  attributes: `arguments`

            - TASK_RAPTOR_MASTER: the task references a raptor master to be
                  instantiated.

              - related  attributes: `raptor_file`
              - related  attributes: `raptor_class`

            - TASK_RAPTOR_WORKER: the task references a raptor worker to be
                  instantiated.

              - related  attributes: `raptor_file`
              - related  attributes: `raptor_class`

            There is a certain overlap between `TASK_EXECUTABLE`, `TASK_SHELL`
            and `TASK_PROC` modes.  As a general rule, `TASK_SHELL` and
            `TASK_PROC` should be used for short running tasks which require a
            single core and no additional resources (gpus, storage, memory).
            `TASK_EXECUTABLE` should be used for all other tasks and is in fact
            the default.  `TASK_SHELL` should only be used if the command to be
            run requires shell specific functionality (e.g., pipes, I/O
            redirection) which cannot easily be mapped to other task attributes.

            TASK_RAPTOR_MASTER and TASK_RAPTOR_WORKER are special types of tasks
            that define RAPTOR's master(s) and worker(s) components and their
            resource requirements. They are launched by the Agent on one or more
            nodes, depending on their requirements.

        executable (str): The executable to launch. The executable is
            expected to be either available via ``$PATH`` on the target
            resource, or to be an absolute path.

        arguments (list[str]): The command line arguments for the given
            `executable` (`list` of `strings`).

        info_pattern (str): A regular expression pattern to extract service
            startup information (only for tasks with mode `TASK_SERVICE`).  The
            pattern is formed like:

                'src:regex'

            where `src` is `stdout`, `stderr`, or a file path, and regex is a
            regular expression to extract the information from the respective
            source.

        code (str): The code to run.  This field is expected to contain valid
            python code which is executed when the task mode is `TASK_EXEC` or
            `TASK_EVAL`.

        function (str): The function to run.  This field is expected to contain
            a python function name which can be resolved in the scope of the
            respective RP worker implementation (see documentation there).  The
            task mode must be set to `TASK_FUNCTION`.  `args` and `kwargs` are
            passed as function parameters.

        args (list, optional): Positional arguments to be passed to the
            `function` (see above).  This field will be serialized  with
            `msgpack` and can thus contain any serializable data types.

        kwargs (dict, optional): Named arguments to be passed to the `function`
            (see above).  This field will be serialized  with `msgpack` and can
            thus contain any serializable data types.

        command (str): A shell command to be executed. This attribute is used
            for the `TASK_SHELL` mode.

        use_mpi (bool, optional): flag if the task should be provided an MPI
            communicator.  Defaults to `True` if more than 1 rank is requested
            (see `ranks`), otherwise defaults to `False`.  Set this to `True` if
            you want to enfoce an MPI communicator on single-ranked tasks.

        ranks (int, optional): The number of application processes to start
            on CPU cores. Default 1.

            For two ranks or more, an MPI communicator will be available to the
            processes.

            `ranks` replaces the deprecated attribute `cpu_processes`.  The
            attribute `cpu_process_type` was previously used to signal the need
            for an MPI communicator - that attribute is now also deprecated and
            will be ignored.

        ranks_per_node (int, optional): The number of ranks to start on each
            node.  If not set, the number of ranks per node will be determined
            by the scheduler depending on resource availability.

        cores_per_rank (int, optional): The number of cpu cores each process
            will have available to start its own threads or processes on.  By
            default, `core` refers to a physical CPU core - but if the pilot has
            been launched with SMT-settings > 1, `core` will refer to a virtual
            core or hyperthread instead (the name depends on the CPU vendor).

            `cores_per_rank` replaces the deprecated attribute `cpu_threads`.

        threading_type (str, optional): The thread type, influences startup and
            environment (`<empty>/POSIX`, `OpenMP`).

            `threading_type` replaces the deprecated attribute
            `cpu_thread_type`.

        gpus_per_rank (float, optional): The number of gpus made available to
            each rank. If gpu is shared among several ranks, then a fraction of
            gpu should be provided (e.g., 2 ranks share a GPU, then
            *gpus_per_rank=.5*).

            `gpus_per_rank` replaces the deprecated attribute `gpu_processes`.
            The attributes `gpu_threads` and `gpu_process_type` are also
            deprecated and will be ignored.

        gpu_type (str, optional): The type of GPU environment to provide to
            the ranks (`<empty>`, `CUDA`, `ROCm`).

            `gpu_type` replaces the deprecated attribute `gpu_thread_type`.

        lfs_per_rank (int, optional): Local File Storage per rank - amount of
            data (MB) required on the local file system of the node.

            `lfs_per_rank` replaces the deprecated attribute `lfs_per_process`.

        mem_per_rank (int, optional): Amount of physical memory required per
            rank.

            `mem_per_rank` replaces the deprecated attribute `mem_per_process`.

        environment (dict, optional): Environment variables to set in the
            environment before the execution (launching picked `LaunchMethod`).

        named_env (str, optional): A named virtual environment as prepared by
            the pilot. The task will remain in `AGENT_SCHEDULING` state until
            that environment gets created.

        sandbox (str, optional): This specifies the working directory of the
            task.  It will be created if it does not exist. By default, the
            sandbox has the name of the task's uid and is relative to the
            pilot's sandbox.

        stdout (str, optional): The name of the file to store stdout. If
            not set then :file:`{uid}.out` will be used.

        stderr (str, optional): The name of the file to store stderr. If
            not set then :file:`{uid}.err` will be used.

        input_staging (list, optional): The files that need to be staged before
            the execution (`list` of `staging directives`, see below).

        output_staging (list, optional): The files that need to be staged after
            the execution (`list` of `staging directives`, see below).

        stage_on_error (bool, optional): Output staging is normally skipped on
            `FAILED` or `CANCELED` tasks, but if this flag is set, staging is
            attempted either way. This may though lead to additional errors if
            the tasks did not manage to produce the expected output files to
            stage. Default False.

        priority: (int, optional): The priority of the task.  Tasks with higher
            priority will be scheduled first.  The default priority is 0.
            Note that task priorities are not guaranteed to be enforced
            strictly, under certain conditions the task may be started later
            than lower priority tasks.

        pre_launch (list, optional): Actions (shell commands) to perform
            before launching (i.e., before LaunchMethod is submitted),
            potentially on a batch node which is different from the node the
            task is placed on.

            Note that the set of shell commands given here are expected to load
            environments, check for work directories and data, etc. They are not
            expected to consume any significant amount of CPU time or other
            resources! Deviating from that rule will likely result in reduced
            overall throughput.

        post_launch (list, optional): Actions (shell commands) to perform
            after launching (i.e., after LaunchMethod is executed).

            Precautions are the same as for `pre_launch` actions.

        pre_exec (list, optional): Actions (shell commands) to perform
            before the task starts (LaunchMethod is submitted, but no actual
            task running yet). Each item could be either a string (`str`), which
            represents an action applied to all ranks, or a dictionary (`dict`),
            which represents a list of actions applied to specified ranks (key
            is a rankID and value is a list of actions to be performed for this
            rank).

            The actions/commands are executed on the respective nodes where the
            ranks are placed, and the actual rank startup will be delayed until
            all `pre_exec` commands have completed.

            Precautions are the same as for `pre_launch` actions.

            No assumption should be made as to where these commands are executed
            (although RP attempts to perform them in the task's execution
            environment).

            No assumption should be made on the specific shell environment the
            commands are executed in other than a POSIX shell environment.

            Errors in executing these commands will result in the task to enter
            `FAILED` state, and no execution of the actual workload will be
            attempted.

        pre_exec_sync (bool, optional): Flag indicates necessary to sync ranks
            execution, which enforce to delay individual rank execution, until
            all `pre_exec` actions for all ranks are completed. Default False.

        post_exec (list, optional): Actions (shell commands) to perform
            after the task finishes. The same remarks as on `pre_exec` apply,
            inclusive the point on error handling, which again will cause the
            task to fail, even if the actual execution was successful.

        restartable (bool, optional): If the task starts to execute on a pilot,
            but cannot finish because the pilot fails or is canceled, the task
            can be restarted. Default False.

        tags (dict, optional): Configuration specific tags, which
            influence task scheduling and execution (e.g., tasks co-location).

        scheduler (str, optional): deprecated in favor of `raptor_id`.

        raptor_id (str, optional): Raptor master ID this task is associated
            with.

        worker_class (str, optional): deprecated in favor of `raptor_class`
            master or worker task.

        raptor_class (str, optional): Class name to instantiate for this Raptor
            master or worker task.

        worker_file (str, optional): deprecated in favor of `raptor_class`.

        raptor_file (str, optional): Optional application supplied Python
            source file to load `raptor_class` from.

        metadata (dict, optional): User defined metadata. Default None.

        services ([str], optional): list of service names the task wants to use.
            If the services are up and running, an envvariable `RP_INFO_XXX`
            will be set where `XXX` is the uppercased service name, and the
            variable's value is the service information obtained by the agent
            during service startup.

        timeout (float, optional): Any timeout larger than 0 will result in
            the task process to be killed after the specified amount of seconds,
            starting from the launch time, if startup_timeout is not set, or
            starting from execution start time, if startup_timeout is set and
            was tracked. The task will then end up in `CANCELED` state.

        startup_timeout (float, optional): Any value larger than 0 will abort
            the task process if it hasn't started the actual execution process,
            e.g., batch system didn't provide allocated resources or the service
            startup takes too long. This option is very important for service
            tasks. The service task will end up in `FAILED` state, regular task
            will end up in `CANCELED` state.

        cleanup (bool, optional): If cleanup flag is set, the pilot will
            delete the entire task sandbox upon termination. This includes all
            generated output data in that sandbox. Output staging will be
            performed before cleanup. Note that task sandboxes are also deleted
            if the pilot's own `cleanup` flag is set. Default False.

        pilot (str, optional): Pilot `uid`. If specified, the task is
            submitted to the pilot with the given ID. If that pilot is not known
            to the TaskManager, an exception is raised.

        slots (radical.pilot.Slots, optional): information on where exactly each
            rank of the task should be placed.

        partition (int, optional): index of pilot partition to use to run that
            task.

    **Task Ranks**

    The notion of `ranks` is central to RP's `TaskDescription` class.  We here
    use the same notion as MPI, in that the number of `ranks` refers to the
    number of individual processes to be spawned by the task execution backend.
    These processes will be near-exact copies of each other: they run in the
    same workdir and the same `environment`, are defined by the same
    `executable` and `arguments`, get the same amount of resources allocated,
    etc.  Notable exceptions are:

    - Rank processes may run on different nodes;
    - rank processes can communicate via MPI;
    - each rank process obtains a unique rank ID.

    It is up to the underlying MPI implementation to determine the exact value
    of the process' rank ID.  The MPI implementation may also set a number of
    additional environment variables for each process.

    It is important to understand that only applications which make use of MPI
    should have more than one rank -- otherwise identical copies of the *same*
    application instance are launched which will compute the same results, thus
    wasting resources for all ranks but one.  Worse: I/O-routines of these
    non-MPI ranks can interfere with each other and invalidate those results.

    Also, applications with a single rank cannot make effective use of MPI---
    depending on the specific resource configuration, RP may launch those tasks
    without providing an MPI communicator.

    **Task Environment**

    RP tasks are expected to be executed in isolation, meaning that their
    runtime environment is completely independent from the environment of other
    tasks, independent from the launch mechanism used to start the task, and
    also independent from the environment of the RP stack itself.

    The task description provides several hooks to help setting up the
    environment in that context.  It is important to understand the way those
    hooks interact with respect to the environments mentioned above.

    - `pre_launch` directives are set and executed before the task is passed on
      to the task launch method.  As such, `pre_launch` usually executed on the
      node where RP's agent is running, and *not* on the tasks target node.
      Executing `pre_launch` directives for many tasks can thus negatively
      impact RP's performance (*).  Note also that `pre_launch` directives can
      in some cases interfere with the launch method.

      Use `pre_launch` directives for rare, heavy-weight operations which
      prepare the runtime environment for multiple tasks: fetch data from a
      remote source, unpack input data, create global communication channels,
      etc.
    - `pre_exec` directives are set and executed *after* the launch method
      placed the task on the compute nodes and are thus running on the target
      node.  Note that for MPI tasks, the `pre_exec` directives are executed
      once per rank.  Running large numbers of `pre_exec` directives
      concurrently can lead to system performance degradation (*), for example
      when those  directives concurrently hot the shared files system (for
      loading modules or Python virtualenvs etc).

      Use `pre_exec` directives for task environment setup such as `module
      load`, `virtualenv activate`, `export` whose effects are expected to be
      applied either to all task ranks or to specified ranks.  Avoid file
      staging operations at this point (files would be redundantly staged
      multiple times - once per rank).

    (*) The performance impact of repeated concurrent access to the system's
    shared file system can be significant and can pose a major bottleneck for
    your application.  Specifically `module load` and `virtualenv activate`
    operations and the like are heavy on file system I/O, and executing those
    for many tasks is ill advised.  Having said that: RP attempts to optimize
    those operations: if it identifies that identical `pre_exec` directives are
    shared between multiple tasks, RP will execute the directives exactly *once*
    and will cache the resulting environment settings - those cached settings
    are then applied to all other tasks with the same directives, without
    executing the directives again.

    **Staging Directives**

    The Staging Directives are specified using a dict in the following form::

          staging_directive = {
             'source'  : None, # see 'Location' below
             'target'  : None, # see 'Location' below
             'action'  : None, # See 'Action operators' below
             'flags'   : None, # See 'Flags' below
             'priority': 0     # Control ordering of actions (unused)
          }

    *Locations*

    `source` and `target` locations can be given as strings or `ru.Url`
    instances.  Strings containing `://` are converted into URLs immediately.
    Otherwise, they are considered absolute or relative paths and are then
    interpreted in the context of the client's working directory.

    .. rubric:: Special URL schemas

    - ``client://``   : relative to the client's working directory
    - ``resource://`` : relative to the RP    sandbox on the target resource
    - ``pilot://``    : relative to the pilot sandbox on the target resource
    - ``task://``     : relative to the task  sandbox on the target resource

    In all these cases, the `hostname` element of the URL is expected to be
    empty, and the path is *always* considered relative to the locations
    specified above (even though URLs usually don't have a notion of relative
    paths).

    For more details on path and sandbox handling check the documentation of
    :func:`radical.pilot.staging_directives.complete_url`.

    *Action operators*

    - rp.TRANSFER : remote file transfer from `source` to `target` URL (client)
    - rp.COPY     : local file copy, i.e., not crossing host boundaries
    - rp.MOVE     : local file move
    - rp.LINK     : local file symlink
    - rp.DOWNLOAD : fetch remote file from `source` URL to `target` URL (agent)

    *Flags*

    - rp.CREATE_PARENTS: create the directory hierarchy for targets on the fly
    - rp.RECURSIVE: if `source` is a directory, handles it recursively
    """

    _schema = {
        UID             : str         ,
        NAME            : str         ,
        MODE            : str         ,

        EXECUTABLE      : str         ,
        ARGUMENTS       : [str]       ,
        INFO_PATTERN    : str         ,
        CODE            : str         ,
        FUNCTION        : str         ,
        ARGS            : [None]      ,
        KWARGS          : {str: None} ,
        COMMAND         : str         ,

        SANDBOX         : str         ,
        ENVIRONMENT     : {str: str}  ,
        NAMED_ENV       : str         ,
        PRE_LAUNCH      : [str]       ,
        PRE_EXEC        : [None]      ,
        PRE_EXEC_SYNC   : bool        ,
        POST_LAUNCH     : [str]       ,
        POST_EXEC       : [None]      ,
        STDOUT          : str         ,
        STDERR          : str         ,
        INPUT_STAGING   : [None]      ,
        OUTPUT_STAGING  : [None]      ,
        STAGE_ON_ERROR  : bool        ,
        PRIORITY        : int         ,

        USE_MPI         : bool        ,
        RANKS           : int         ,
        RANKS_PER_NODE  : int         ,
        CORES_PER_RANK  : int         ,
        GPUS_PER_RANK   : float       ,
        THREADING_TYPE  : str         ,
        GPU_TYPE        : str         ,
        LFS_PER_RANK    : int         ,
        MEM_PER_RANK    : int         ,

        # deprecated
        CPU_PROCESSES   : int         ,  # RANKS
        CPU_PROCESS_TYPE: str         ,  # n/a
        CPU_THREADS     : int         ,  # CORES_PER_RANK
        CPU_THREAD_TYPE : str         ,  # THREADING_TYPE
        GPU_PROCESSES   : int         ,  # GPUS_PER_RANK
        GPU_PROCESS_TYPE: str         ,  # GPU_TYPE
        GPU_THREADS     : int         ,  # n/a
        GPU_THREAD_TYPE : str         ,  # n/a
        LFS_PER_PROCESS : int         ,  # LFS_PER_RANK
        MEM_PER_PROCESS : int         ,  # MEM_PER_RANK
        SCHEDULER       : str         ,  # RAPTOR_ID
        WORKER_FILE     : str         ,  # RAPTOR_FILE
        WORKER_CLASS    : str         ,  # RAPTOR_CLASS

        RESTARTABLE     : bool        ,
        TAGS            : {None: None},
        RAPTOR_ID       : str         ,
        RAPTOR_FILE     : str         ,
        RAPTOR_CLASS    : str         ,
        METADATA        : {str: None} ,
        SERVICES        : [str]       ,
        TIMEOUT         : float       ,
        STARTUP_TIMEOUT : float       ,
        CLEANUP         : bool        ,
        PILOT           : str         ,
        SLOTS           : [Slot]      ,
        PARTITION       : int         ,
    }

    _defaults = {
        UID             : ''          ,
        NAME            : ''          ,
        MODE            : TASK_EXECUTABLE,
        EXECUTABLE      : ''          ,
        ARGUMENTS       : list()      ,
        INFO_PATTERN    : ''          ,
        CODE            : ''          ,
        FUNCTION        : ''          ,
        ARGS            : list()      ,
        KWARGS          : dict()      ,
        COMMAND         : ''          ,

        SANDBOX         : ''          ,
        ENVIRONMENT     : dict()      ,
        NAMED_ENV       : ''          ,
        PRE_LAUNCH      : list()      ,
        PRE_EXEC        : list()      ,
        PRE_EXEC_SYNC   : False       ,
        POST_LAUNCH     : list()      ,
        POST_EXEC       : list()      ,
        STDOUT          : ''          ,
        STDERR          : ''          ,
        INPUT_STAGING   : list()      ,
        OUTPUT_STAGING  : list()      ,
        STAGE_ON_ERROR  : False       ,
        PRIORITY        : 0           ,

        USE_MPI         : None        ,
        RANKS           : 1           ,
        RANKS_PER_NODE  : None        ,
        CORES_PER_RANK  : 1           ,
        GPUS_PER_RANK   : 0.          ,
        THREADING_TYPE  : ''          ,
        GPU_TYPE        : ''          ,
        LFS_PER_RANK    : 0           ,
        MEM_PER_RANK    : 0           ,

        # deprecated
        CPU_PROCESSES   : 0           ,
        CPU_PROCESS_TYPE: ''          ,
        CPU_THREADS     : 0           ,
        CPU_THREAD_TYPE : ''          ,
        GPU_PROCESSES   : 0           ,
        GPU_PROCESS_TYPE: ''          ,
        GPU_THREADS     : 0           ,
        GPU_THREAD_TYPE : ''          ,
        LFS_PER_PROCESS : 0           ,
        MEM_PER_PROCESS : 0           ,
        SCHEDULER       : ''          ,
        WORKER_FILE     : ''          ,
        WORKER_CLASS    : ''          ,

        RESTARTABLE     : False       ,
        TAGS            : dict()      ,
        RAPTOR_ID       : ''          ,
        RAPTOR_FILE     : ''          ,
        RAPTOR_CLASS    : ''          ,
        METADATA        : dict()      ,
        SERVICES        : list()      ,
        TIMEOUT         : 0.0         ,
        STARTUP_TIMEOUT : 0.0         ,
        CLEANUP         : False       ,
        PILOT           : ''          ,
        SLOTS           : list()      ,
        PARTITION       : None        ,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        if not self.get('mode'):
            self['mode'] = TASK_EXECUTABLE

        if self.mode in [TASK_EXECUTABLE, TASK_SERVICE, AGENT_SERVICE]:
            if not self.get('executable'):
                umode = self.mode.upper().replace('.', '_')
                raise ValueError("%s Task mode needs 'executable'" % umode)

        elif self.mode in [TASK_FUNC, TASK_METH]:
            if not self.get('function'):
                raise ValueError("TASK_FUNC Task mode needs 'function'")
            if self.get('named_env'):
                raise ValueError("TASK_FUNC and TASK_METH Task mode does not "
                                 "support 'named_env'")

        elif self.mode == TASK_PROC:
            if not self.get('executable'):
                raise ValueError("TASK_PROC Task mode needs 'executable'")

        elif self.mode == TASK_EVAL:
            if not self.get('code'):
                raise ValueError("TASK_EVAL Task mode needs 'code'")

        elif self.mode == TASK_EXEC:
            if not self.get('code'):
                raise ValueError("TASK_EXEC Task mode needs 'code'")

        elif self.mode == TASK_SHELL:
            if not self.get('command'):
                raise ValueError("TASK_SHELL Task mode needs 'command'")

        # backward compatibility for deprecated attributes
        if self.cpu_processes:
            self.ranks = self.cpu_processes
            self.cpu_processes = 0

        if self.cpu_threads:
            self.cores_per_rank = self.cpu_threads
            self.cpu_threads = 0

        if self.cpu_thread_type:
            self.threading_type = self.cpu_thread_type
            self.cpu_thread_type = None

        if self.gpu_processes:
            self.gpus_per_rank = float(self.gpu_processes)
            self.gpu_processes = 0

        if self.gpu_process_type:
            self.gpu_type = self.gpu_process_type
            self.gpu_process_type = None

        if self.lfs_per_process:
            self.lfs_per_rank = self.lfs_per_process
            self.lfs_per_process = 0

        if self.mem_per_process:
            self.mem_per_rank = self.mem_per_process
            self.mem_per_process = 0

        if self.scheduler:
            self.raptor_id = self.scheduler
            self.scheduler = ''

        if self.worker_file:
            self.raptor_file = self.worker_file
            self.worker_file = ''

        if self.worker_class:
            self.raptor_class = self.worker_class
            self.raptor_class = ''

        if self.use_mpi is None:
            self.use_mpi = bool(self.ranks - 1)

        # deprecated and ignored
        if self.cpu_process_type: pass
        if self.gpu_threads     : pass
        if self.gpu_thread_type : pass

      # if self.mode in [TASK_SHELL, TASK_PROC]:
      #
      #     if self.get('cpu_processes', 1) * self.get('cpu_threads', 1) > 1:
      #         raise ValueError("TASK_SHELL and TASK_PROC Tasks must be single core")
      #
      #     if self.get('gpu_processes', 0) > 0:
      #         raise ValueError("TASK_SHELL and TASK_PROC Tasks canont use GPUs")


# ------------------------------------------------------------------------------
