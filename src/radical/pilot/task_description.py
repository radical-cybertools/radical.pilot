
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

# task modes
TASK_EXECUTABLE  = 'executable'
TASK_FUNCTION    = 'function'
TASK_METHOD      = 'method'
TASK_EVAL        = 'eval'
TASK_EXEC        = 'exec'
TASK_PROC        = 'proc'
TASK_SHELL       = 'shell'

# task description attributes
UID              = 'uid'
NAME             = 'name'
MODE             = 'mode'

# mode: TASK_EXECUTABLE
EXECUTABLE       = 'executable'
ARGUMENTS        = 'arguments'

# mode: TASK_METHOD  # FIXME
METHOD           = 'method'
ARGS             = 'args'
KWARGS           = 'kwargs'

# mode: TASK_FUNCTION
FUNCTION         = 'function'
ARGS             = 'args'
KWARGS           = 'kwargs'

# mode: TASK_EXEC, TASK_EVAL
CODE             = 'code'

# mode: TASK_PROC, TASK_SHELL
COMMAND          = 'command'

# environment
ENVIRONMENT      = 'environment'
NAMED_ENV        = 'named_env'
SANDBOX          = 'sandbox'

# resource requirements
CPU_PROCESSES    = 'cpu_processes'            # ranks
CPU_PROCESS_TYPE = 'cpu_process_type'         # MPI?
CPU_THREADS      = 'cpu_threads'              # cores per rank
CPU_THREAD_TYPE  = 'cpu_thread_type'          # OpenMP?

GPU_PROCESSES    = 'gpu_processes'            # gpus per rank
GPU_PROCESS_TYPE = 'gpu_process_type'         # CUDA?
GPU_THREADS      = 'gpu_threads'              # part of gpu?
GPU_THREAD_TYPE  = 'gpu_thread_type'          # ?

LFS_PER_PROCESS  = 'lfs_per_process'          # disk space per rank
MEM_PER_PROCESS  = 'mem_per_process'          # memory per rank

INPUT_STAGING    = 'input_staging'
OUTPUT_STAGING   = 'output_staging'
STAGE_ON_ERROR   = 'stage_on_error'
PRE_LAUNCH       = 'pre_launch'
PRE_EXEC         = 'pre_exec'
PRE_RANK         = 'pre_rank'
POST_LAUNCH      = 'post_launch'
POST_EXEC        = 'post_exec'
POST_RANK        = 'post_rank'
CLEANUP          = 'cleanup'
PILOT            = 'pilot'
STDOUT           = 'stdout'
STDERR           = 'stderr'
RESTARTABLE      = 'restartable'
SCHEDULER        = 'scheduler'
TAGS             = 'tags'
METADATA         = 'metadata'


# ------------------------------------------------------------------------------
#
class TaskDescription(ru.TypedDict):
    """
    A TaskDescription object describes the requirements and properties
    of a :class:`radical.pilot.Task` and is passed as a parameter to
    :meth:`radical.pilot.TaskManager.submit_tasks` to instantiate and run
    a new task.

    .. note:: A TaskDescription **MUST** define at least an
              `executable` -- all other elements are optional.

    .. data:: uid

       [type: `str` | default: `""`] A unique ID for the task. This attribute
       is optional, a unique ID will be assigned by RP if the field is not set.

    .. data:: name

       [type: `str` | default: `""`] A descriptive name for the task. This
       attribute can be used to map individual tasks back to application level
       workloads.

    .. data:: mode

       [type: `str` | default: `"executable"`] The execution mode to be used for
       this task.  The following modes are accepted:

         - TASK_EXECUTABLE: the task is spawned as an external executable via a
           resource specific launch method (srun, aprun, mpiexec, etc).
           required attributes: `executable`
           related  attributes: `arguments`

         - TASK_FUNCTION: the task references a python function to be called.
           required attributes: `function`
           related  attributes: `args`
           related  attributes: `kwargs`

         - TASK_METHOD: the task references a raptor worker method to be
           called.
           required attributes: `method`
           related  attributes: `args`
           related  attributes: `kwargs`

         - TASK_EVAL: the task is a code snippet to be evaluated.
           required attributes: `code`

         - TASK_EXEC: the task is a code snippet to be `exec`'ed.
           required attributes: `code`

         - TASK_SHELL: the task is a shell command line to be run.
           required attributes: `command`

         - TASK_PROC: the task is a single core process to be executed.
           required attributes: `executable`
           related  attributes: `arguments`

        There exists a certain overlap between `TASK_EXECUTABLE`, `TASK_SHELL`
        and `TASK_PROC` modes.  As a general rule, `TASK_SHELL` and `TASK_PROC`
        should be used for short running tasks which require a single core and
        no additional resources (gpus, storage, memory).  `TASK_EXECUTABLE`
        should be used for all other tasks and is in fact the default.
        `TASK_SHELL` should only be used if the command to be run requires shell
        specific functionality (pipes, I/O redirection) which cannot easily be
        mapped to other task attributes.


    .. data:: executable

       [type: `str` | default: `""`] The executable to launch. The executable
       is expected to be either available via `$PATH` on the target resource,
       or to be an absolute path.

    .. data:: arguments

       [type: `list` | default: `[]`] The command line arguments for the given
       `executable` (`list` of `strings`).

    .. data:: code

       [type: `str` | default: `""`] The code to run.  This field is expected to
       contain valid python code which is executed when the task mode is
       `TASK_EXEC` or `TASK_EVAL`.

    .. data:: function

       [type: `str` | default: `""`] The function to run.  This field is
       expected to contain a python function name which can be resolved in the
       scope of the respective RP worker implementation (see documentation
       there).  The task mode must be set to `TASK_FUNCTION`.  `args` and
       `kwargs` are passed as function parameters.

    .. data:: args

       [type: `list` | default: `[]`] Unnamed arguments to be passed to the
       `function` (see above).  This field will be serialized  with `msgpack`
       and can thus contain any serializable data types.

    .. data:: kwargs

       [type: `dict` | default: `{}`] Named arguments to be passed to the
       `function` (see above).  This field will be serialized  with `msgpack`
       and can thus contain any serializable data types.

    .. data:: command

       [type: `str` | default: `""`] A shell command to be executed.  This
       attribute is used for the `TASK_SHELL` mode.

    .. data:: cpu_processes

       [type: `int` | default: `1`] The number of application processes to start
       on CPU cores.

    .. data:: cpu_threads

       [type: `int` | default: `1`] The number of threads each process will
       start on CPU cores.

    .. data:: cpu_process_type

       [type: `str` | default: `""`] The process type, determines startup
       method (`<empty>/POSIX`, `MPI`).

    .. data:: cpu_thread_type

       [type: `str` | default: `""`] The thread type, influences startup and
       environment (`<empty>/POSIX`, `OpenMP`).

    .. data:: gpu_processes

       [type: `int` | default: `0`] The number of application processes to
       start on GPU cores.

    .. data:: gpu_threads

       [type: `int` | default: `1`] The number of threads each process will
       start on GPU cores.

    .. data:: gpu_process_type

       [type: `str` | default: `""`] The process type, determines startup
       method (`<empty>/POSIX`, `MPI`).

    .. data:: gpu_thread_type

       [type: `str` | default: `""`] The thread type, influences startup and
       environment (`<empty>/POSIX`, `OpenMP`, `CUDA`).

    .. data:: lfs_per_process

       [type: `int` | default: `0`] Local File Storage per process - amount of
       data (MB) required on the local file system of the node.

    .. data:: mem_per_process

       [type: `int` | default: `0`] Amount of physical memory required per
       process.

    .. data:: environment

       [type: `dict` | default: `{}`] Environment variables to set in the
       environment before the execution (launching picked `LaunchMethod`).

    .. data:: named_env

       [type: `str` | default: `""`] A named virtual environment as prepared by
       the pilot. The task will remain in `AGENT_SCHEDULING` state until that
       environment gets created.

    .. data:: sandbox

       [type: `str` | default: `""`] This specifies the working directory of
       the task. That directory *MUST* be relative to the pilot sandbox. It
       will be created if it does not exist. By default, the sandbox has
       the name of the task's uid.

    .. data:: stdout

       [type: `str` | default: `""`] The name of the file to store stdout. If
       not set then the name of the following format will be used: `<uid>.out`.

    .. data:: stderr

       [type: `str` | default: `""`] The name of the file to store stderr. If
       not set then the name of the following format will be used: `<uid>.err`.

    .. data:: input_staging

       [type: `list` | default: `[]`] The files that need to be staged before
       the execution (`list` of `staging directives`, see below).

    .. data:: output_staging

       [type: `list` | default: `[]`] The files that need to be staged after
       the execution (`list` of `staging directives`, see below).

    .. data:: stage_on_error

       [type: `bool` | default: `False`] Output staging is normally skipped on
       `FAILED` or `CANCELED` tasks, but if this flag is set, staging is
       attempted either way. This may though lead to additional errors if the
       tasks did not manage to produce the expected output files to stage.

    .. data:: pre_exec

       [type: `list` | default: `[]`] Actions (shell commands) to perform before
       this task starts. Note that the set of shell commands given here are
       expected to load environments, check for work directories and data, etc.
       They are not expected to consume any significant amount of CPU time or
       other resources! Deviating from that rule will likely result in reduced
       overall throughput.

       No assumption should be made as to where these commands are executed
       (although RP attempts to perform them in the task's execution
       environment).

       No assumption should be made on the specific shell environment the
       commands are executed in.

       Errors in executing these commands will result in the task to enter
       `FAILED` state, and no execution of the actual workload will be
       attempted.

    .. data:: pre_launch

       [type: `list` | default: `[]`] Like `pre_exec`, but runs befor launching,
       potentially on a batch node which is different from the node the task is
       placed on.

    .. data:: pre_rank

       [type: `dict` | default: `{}`] A dictionary which maps a set of
       `pre_exec` like commands to each rank.  The commands are executed on the
       respective nodes where the ranks are places, and the actual rank startup
       will be delayed until all `pre_rank` commands have completed.

    .. data:: post_exec

       [type: `list` | default: `[]`] Actions (shell commands) to perform after
       this task finishes. The same remarks as on `pre_exec` apply, inclusive
       the point on error handling, which again will cause the task to fail,
       even if the actual execution was successful.

    .. data:: post_launch

       ...

    .. data:: post_rank

       ...

    .. data:: restartable

       [type: `bool` | default: `False`] If the task starts to execute on
       a pilot, but cannot finish because the pilot fails or is canceled,
       the task can be restarted.

    .. data:: scheduler

       Request the task to be handled by a specific agent scheduler


    .. data:: tags

       [type: `dict` | default: `{}`] Configuration specific tags, which
       influence task scheduling and execution (e.g., tasks co-location).

    .. data:: metadata

       [type: `ANY` | default: `None`] User defined metadata.

    .. data:: cleanup

       [type: `bool` | default: `False`] If cleanup flag is set, the pilot will
       delete the entire task sandbox upon termination. This includes all
       generated output data in that sandbox. Output staging will be performed
       before cleanup. Note that task sandboxes are also deleted if the pilot's
       own `cleanup` flag is set.

    .. data:: pilot

       [type: `str` | default: `""`] Pilot `uid`, if specified, the task is
       submitted to the pilot with the given ID. If that pilot is not known to
       the TaskManager, an exception is raised.


    Staging Directives
    ==================

    The Staging Directives are specified using a dict in the following form::

          staging_directive = {
             'source'  : None, # see 'Location' below
             'target'  : None, # see 'Location' below
             'action'  : None, # See 'Action operators' below
             'flags'   : None, # See 'Flags' below
             'priority': 0     # Control ordering of actions (unused)
          }


    Locations
    ---------

      `source` and `target` locations can be given as strings or `ru.Url`
      instances.  Strings containing `://` are converted into URLs immediately.
      Otherwise they are considered absolute or relative paths and are then
      interpreted in the context of the client's working directory.

      Special URL schemas:

        * `client://`   : relative to the client's working directory
        * `resource://` : relative to the RP    sandbox on the target resource
        * `pilot://`    : relative to the pilot sandbox on the target resource
        * `task://`     : relative to the task  sandbox on the target resource

      In all these cases, the `hostname` element of the URL is expected to be
      empty, and the path is *always* considered relative to the locations
      specified above (even though URLs usually don't have a notion of relative
      paths).


    Action operators
    ----------------

      Action operators:

        * rp.TRANSFER : remote file transfer from `source` URL to `target` URL
        * rp.COPY     : local file copy, i.e., not crossing host boundaries
        * rp.MOVE     : local file move
        * rp.LINK     : local file symlink


    Flags
    -----

      Flags:

        * rp.CREATE_PARENTS : create the directory hierarchy for targets on
          the fly
        * rp.RECURSIVE      : if `source` is a directory, handles it recursively


    Task Environment
    ================

    RP tasks are expected to be executed in isolation, meaning that their
    runtime environment is completely independent from the environment of other
    tasks, independent from the launch mechanism used to start the task, and
    also independent from the environment of the RP stack itself.

    The task description provides several hooks to help setting up the
    environment in that context.  It is important to understand the way those
    hooks interact with respect to the environments mentioned above.

      - `pre_launch` directives are set and executed before the task is passed
        on to the task launch method.  As such, `pre_launch` usually executed
        on the node where RP's agent is running, and *not* on the tasks target
        node.  Executing `pre_launch` directives for many tasks can thus
        negatively impact RP's performance (*).  Note also that `pre_launch`
        directives can in some cases interfere with the launch method.

        Use `pre_launch` directives for rare, heavy-weight operations which
        prepare the runtime environment for multiple tasks: fetch data from
        a remote source, unpack input data, create global communication
        channels, etc.

      - `pre_exec` directives are set and executed *after* the launch method
        placed the task on the compute nodes and are thus running on the target
        node.  Note that for MPI tasks, the `pre_exec` directives are executed
        once per rank.  Running large numbers of `pre_exec` directives
        concurrently can lead to system performance degradation (*), for example
        when those  directives concurrently hot the shared files system (for
        loading modules or Python virtualenvs etc).

        Use `pre_exec` directives for task environment setup such as `module
        load`, `virtualenv activate`, `export` whose effects are expected to be
        applied to all task ranks.  Avoid file staging operations at this point
        (files would be redundantly staged multiple times - once per rank).

      - `pre_rank` directives are executed only for the specified rank.  Note
        that environment settings specified in `pre_rank` will thus only apply
        to that specific rank, not to other ranks.  All other ranks stall until
        the last `pre_rank` directive has been completed -- only then is the
        actual workload task being executed on all ranks.

        Use `pre_rank` directives on rank 0 to prepare task input data: the
        operations are performed once per task, and all ranks will stall until
        the directives are completed, i.e., until input data are available.
        Avoid using `pre_rank` directives for environment settings - `pre_exec`
        will generally perform better in this case.

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
    """

    _schema = {
        UID             : str         ,
        NAME            : str         ,
        MODE            : str         ,

        EXECUTABLE      : str         ,
        ARGUMENTS       : [str]       ,
        CODE            : str         ,
        FUNCTION        : str         ,
        ARGS            : [None]      ,
        KWARGS          : {str: None} ,
        COMMAND         : str         ,

        SANDBOX         : str         ,
        ENVIRONMENT     : {str: str}  ,
        NAMED_ENV       : str         ,
        PRE_LAUNCH      : [str]       ,
        PRE_EXEC        : [str]       ,
        PRE_RANK        : {int: None} ,
        POST_LAUNCH     : [str]       ,
        POST_EXEC       : [str]       ,
        POST_RANK       : {int: None} ,
        STDOUT          : str         ,
        STDERR          : str         ,
        INPUT_STAGING   : [None]      ,
        OUTPUT_STAGING  : [None]      ,
        STAGE_ON_ERROR  : bool        ,

        CPU_PROCESSES   : int         ,
        CPU_PROCESS_TYPE: str         ,
        CPU_THREADS     : int         ,
        CPU_THREAD_TYPE : str         ,
        GPU_PROCESSES   : int         ,
        GPU_PROCESS_TYPE: str         ,
        GPU_THREADS     : int         ,
        GPU_THREAD_TYPE : str         ,
        LFS_PER_PROCESS : int         ,
        MEM_PER_PROCESS : int         ,

        RESTARTABLE     : bool        ,
        SCHEDULER       : str         ,
        TAGS            : {None: None},
        METADATA        : None        ,
        CLEANUP         : bool        ,
        PILOT           : str         ,
    }

    _defaults = {
        UID             : ''          ,
        NAME            : ''          ,
        MODE            : 'executable',
        EXECUTABLE      : ''          ,
        ARGUMENTS       : list()      ,
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
        PRE_RANK        : dict()      ,
        POST_LAUNCH     : list()      ,
        POST_EXEC       : list()      ,
        POST_RANK       : dict()      ,
        STDOUT          : ''          ,
        STDERR          : ''          ,
        INPUT_STAGING   : list()      ,
        OUTPUT_STAGING  : list()      ,
        STAGE_ON_ERROR  : False       ,

        CPU_PROCESSES   : 1           ,
        CPU_PROCESS_TYPE: ''          ,
        CPU_THREADS     : 1           ,
        CPU_THREAD_TYPE : ''          ,
        GPU_PROCESSES   : 0           ,
        GPU_PROCESS_TYPE: ''          ,
        GPU_THREADS     : 1           ,
        GPU_THREAD_TYPE : ''          ,
        LFS_PER_PROCESS : 0           ,
        MEM_PER_PROCESS : 0           ,

        RESTARTABLE     : False       ,
        SCHEDULER       : ''          ,
        TAGS            : dict()      ,
        METADATA        : None        ,
        CLEANUP         : False       ,
        PILOT           : ''          ,
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


        if self.mode == TASK_EXECUTABLE:
            if not self.get('executable'):
                raise ValueError("TASK_EXECUTABLE Task needs 'executable'")

        elif self.mode == TASK_FUNCTION:
            if not self.get('function'):
                raise ValueError("TASK_FUNCTION Task needs 'function'")

        elif self.mode == TASK_PROC:
            if not self.get('executable'):
                raise ValueError("TASK_PROC Task needs 'executable'")

        elif self.mode == TASK_EVAL:
            if not self.get('code'):
                raise ValueError("TASK_EVAL Task needs 'code'")

        elif self.mode == TASK_EXEC:
            if not self.get('code'):
                raise ValueError("TASK_EXEC Task needs 'code'")

        elif self.mode == TASK_SHELL:
            if not self.get('command'):
                raise ValueError("TASK_SHELL Task needs 'command'")


      # if self.mode in [TASK_SHELL, TASK_PROC]:
      #
      #     if self.get('cpu_processes', 1) * self.get('cpu_threads', 1) > 1:
      #         raise ValueError("TASK_SHELL and TASK_PROC Tasks must be single core")
      #
      #     if self.get('gpu_processes', 0) > 0:
      #         raise ValueError("TASK_SHELL and TASK_PROC Tasks canont use GPUs")


# ------------------------------------------------------------------------------
