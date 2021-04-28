
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru


# ------------------------------------------------------------------------------
# Attribute description keys
#
UID                    = 'uid'
NAME                   = 'name'
EXECUTABLE             = 'executable'
ARGUMENTS              = 'arguments'
ENVIRONMENT            = 'environment'
NAMED_ENV              = 'named_env'
SANDBOX                = 'sandbox'

CPU_PROCESSES          = 'cpu_processes'
CPU_PROCESS_TYPE       = 'cpu_process_type'
CPU_THREADS            = 'cpu_threads'
CPU_THREAD_TYPE        = 'cpu_thread_type'

GPU_PROCESSES          = 'gpu_processes'
GPU_PROCESS_TYPE       = 'gpu_process_type'
GPU_THREADS            = 'gpu_threads'
GPU_THREAD_TYPE        = 'gpu_thread_type'

LFS_PER_PROCESS        = 'lfs_per_process'
MEM_PER_PROCESS        = 'mem_per_process'

INPUT_STAGING          = 'input_staging'
OUTPUT_STAGING         = 'output_staging'
STAGE_ON_ERROR         = 'stage_on_error'
PRE_EXEC               = 'pre_exec'
POST_EXEC              = 'post_exec'
KERNEL                 = 'kernel'
CLEANUP                = 'cleanup'
PILOT                  = 'pilot'
STDOUT                 = 'stdout'
STDERR                 = 'stderr'
RESTARTABLE            = 'restartable'
TAGS                   = 'tags'
METADATA               = 'metadata'

# process / thread types (for both, CPU and GPU processes/threads)
POSIX                  = 'POSIX'   # native threads / application threads
MPI                    = 'MPI'
OpenMP                 = 'OpenMP'
CUDA                   = 'CUDA'
FUNC                   = 'FUNC'
# FIXME: move process/thread types to `radical.pilot.constants`


# ------------------------------------------------------------------------------
#
class TaskDescription(ru.Description):
    """
    A TaskDescription object describes the requirements and properties
    of a :class:`radical.pilot.Task` and is passed as a parameter to
    :meth:`radical.pilot.TaskManager.submit_tasks` to instantiate and run
    a new task.

    .. note:: A TaskDescription **MUST** define at least an
              `executable` or `kernel` -- all other elements are optional.

    .. data:: uid

       [type: `str` | default: `""`] A unique ID for the task. This attribute
       is optional, a unique ID will be assigned by RP if the field is not set.

    .. data:: name

       [type: `str` | default: `""`] A descriptive name for the task. This
       attribute can be used to map individual tasks back to application level
       workloads.

    .. data:: executable

       [type: `str` | default: `""`] The executable to launch. The executable
       is expected to be either available via `$PATH` on the target resource,
       or to be an absolute path.

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

    .. data:: arguments

       [type: `list` | default: `[]`] The command line arguments for the given
       `executable` (`list` of `strings`).

    .. data:: environment

       [type: `dict` | default: `{}`] Environment variables to set in the
       environment before the execution (launching picked `LaunchMethod`).

    .. data:: named_env

       [type: `str` | default: `""`] A named virtual environment as prepared by
       the pilot. The task will fail if that environment does not exist.

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

    .. data:: post_exec

       [type: `list` | default: `[]`] Actions (shell commands) to perform after
       this task finishes. The same remarks as on `pre_exec` apply, inclusive
       the point on error handling, which again will cause the task to fail,
       even if the actual execution was successful.

    .. data:: kernel

       [type: `str` | default: `""`] Name of a simulation kernel, which expands
       to description attributes once the task is scheduled to a pilot and
       resource. `TODO: explain in detail, referencing EnTK.`

    .. data:: restartable

       [type: `bool` | default: `False`] If the task starts to execute on
       a pilot, but cannot finish because the pilot fails or is canceled,
       the task can be restarted.

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

    """

    _schema = {
        UID             : str         ,
        NAME            : str         ,
        EXECUTABLE      : str         ,
        KERNEL          : str         ,
        SANDBOX         : str         ,
        ARGUMENTS       : [str]       ,
        ENVIRONMENT     : {str: str}  ,
        NAMED_ENV       : str         ,
        PRE_EXEC        : [str]       ,
        POST_EXEC       : [str]       ,
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
        TAGS            : {None: None},
        METADATA        : None        ,
        CLEANUP         : bool        ,
        PILOT           : str         ,
    }

    _defaults = {
        UID             : ''          ,
        NAME            : ''          ,
        EXECUTABLE      : ''          ,
        KERNEL          : ''          ,
        SANDBOX         : ''          ,
        ARGUMENTS       : list()      ,
        ENVIRONMENT     : dict()      ,
        NAMED_ENV       : ''          ,
        PRE_EXEC        : list()      ,
        POST_EXEC       : list()      ,
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

        if not self.get('executable') and \
           not self.get('kernel')     :
            raise ValueError("Task description needs 'executable' or 'kernel'")


# ------------------------------------------------------------------------------

