
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# import radical.saga.attributes as rsa


# ------------------------------------------------------------------------------
# Attribute description keys
NAME                   = 'name'
EXECUTABLE             = 'executable'
ARGUMENTS              = 'arguments'
ENVIRONMENT            = 'environment'
TAGS                   = 'tags'

CORES                  = 'cores'  # deprecated

CPU_PROCESSES          = 'cpu_processes'
CPU_PROCESS_TYPE       = 'cpu_process_type'
CPU_THREADS            = 'cpu_threads'
CPU_THREAD_TYPE        = 'cpu_thread_type'

GPU_PROCESSES          = 'gpu_processes'
GPU_PROCESS_TYPE       = 'gpu_process_type'
GPU_THREADS            = 'gpu_threads'
GPU_THREAD_TYPE        = 'gpu_thread_type'

LFS_PER_PROCESS        = 'lfs_per_process'
TAG                    = 'tag'
MEM_PER_PROCESS        = 'mem_per_process'

INPUT_STAGING          = 'input_staging'
OUTPUT_STAGING         = 'output_staging'
PRE_EXEC               = 'pre_exec'
POST_EXEC              = 'post_exec'
KERNEL                 = 'kernel'
CLEANUP                = 'cleanup'
PILOT                  = 'pilot'
STDOUT                 = 'stdout'
STDERR                 = 'stderr'
RESTARTABLE            = 'restartable'
METADATA               = 'metadata'

# process / thread types (for both, CPU and GPU processes/threads)
POSIX                  = 'POSIX'   # native threads / application threads
MPI                    = 'MPI'
OpenMP                 = 'OpenMP'
CUDA                   = 'CUDA'


# ------------------------------------------------------------------------------
#
class ComputeUnitDescription(dict):
    """
    A ComputeUnitDescription object describes the requirements and properties
    of a :class:`radical.pilot.ComputeUnit` and is passed as a parameter to
    :meth:`radical.pilot.UnitManager.submit_units` to instantiate and run
    a new unit.

    - all processes use 1 CPU core.
    - use gpu_processes to request processes which have a GPU allocated
    - use cpu_processes to request processes which have no GPU allocated
    - use cpu_threads to allocate additional cores for each cpu_process
    - use gpu_threads to allocate additional cores for each gpu_process

    .. note:: A ComputeUnitDescription **MUST** define at least an
              `executable` or `kernel` -- all other elements are optional.

    **Example**::

        # TODO 

    .. data:: executable 

       The executable to launch (`string`).  The executable is expected to be
       either available via `$PATH` on the target resource, or to be an absolute
       path.

       default: `None`


    .. data:: cpu_processes    
       number of application processes to start on CPU cores
       default: 0

    .. data:: cpu_threads      
       number of threads each process will start on CPU cores
       default: 1

    .. data:: cpu_process_type 
       process type, determines startup method (POSIX, MPI) 
       default: POSIX

    .. data:: cpu_thread_type  
       thread type, influences startup and environment (POSIX, OpenMP)
       default: POSIX

    .. data:: gpu_processes    
       number of application processes to start on GPU cores
       default: 0

    .. data:: gpu_threads      
       number of threads each process will start on GPU cores
       default: 1

    .. data:: gpu_process_type 
       process type, determines startup method (POSIX, MPI) 
       default: POSIX

    .. data:: gpu_thread_type  
       thread type, influences startup and environment (POSIX, OpenMP, CUDA)
       default: POSIX

    .. data:: lfs (local file storage)
       amount of data (MB) required on the local file system of the node 
       default: 0

    .. data:: name 

       A descriptive name for the compute unit (`string`).  This attribute can
       be used to map individual units back to application level workloads.

       default: `None`


    .. data:: arguments 

       The command line arguments for the given `executable` (`list` of
       `strings`).

       default: `[]`


    .. data:: environment 

       Environment variables to set in the environment before execution
       (`dict`).

       default: `{}`


    .. data:: stdout

       The name of the file to store stdout in (`string`).

       default: `STDOUT`


    .. data:: stderr

       The name of the file to store stderr in (`string`).

       default: `STDERR`


    .. data:: input_staging

       The files that need to be staged before execution (`list` of `staging
       directives`, see below).

       default: `{}`


    .. data:: output_staging

       The files that need to be staged after execution (`list` of `staging
       directives`, see below).

       default: `{}`


    .. data:: pre_exec

       Actions (shell commands) to perform before this task starts (`list` of
       `strings`).  Note that the set of shell commands given here are expected
       to load environments, check for work directories and data, etc.  They are
       not expected to consume any significant amount of CPU time or other
       resources!  Deviating from that rule will likely result in reduced
       overall throughput.

       No assumption should be made as to where these commands are executed
       (although RP attempts to perform them in the unit's execution
       environment).  

       No assumption should be made on the specific shell environment the
       commands are executed in.

       Errors in executing these commands will result in the unit to enter
       `FAILED` state, and no execution of the actual workload will be
       attempted.

       default: `[]`


    .. data:: post_exec

       Actions (shell commands) to perform after this task finishes (`list` of
       `strings`).  The same remarks as on `pre_exec` apply, inclusive the point
       on error handling, which again will cause the unit to fail, even if the
       actual execution was successful..

       default: `[]`


    .. data:: kernel

       Name of a simulation kernel which expands to description attributes once
       the unit is scheduled to a pilot (and resource).

       .. note:: TODO: explain in detail, reference ENMDTK.

       default: `None`


    .. data:: restartable

       If the unit starts to execute on a pilot, but cannot finish because the
       pilot fails or is canceled, can the unit be restarted on a different
       pilot / resource? 

       default: `False`


    .. data:: metadata

       user defined metadata

       default: `None`


    .. data:: cleanup

       If cleanup (a `bool`) is set to `True`, the pilot will delete the entire
       unit sandbox upon termination. This includes all generated output data in
       that sandbox.  Output staging will be performed before cleanup.

       Note that unit sandboxes are also deleted if the pilot's own `cleanup`
       flag is set.

       default: `False`


    .. data:: pilot

       If specified as `string` (pilot uid), the unit is submitted to the pilot
       with the given ID.  If that pilot is not known to the unit manager, an
       exception is raised.


    Staging Directives
    ==================

    The Staging Directives are specified using a dict in the following form:

        staging_directive = {
            'source'  : None, # see 'Location' below
            'target'  : None, # see 'Location' below
            'action'  : None, # See 'Action operators' below
            'flags'   : None, # See 'Flags' below
            'priority': 0     # Control ordering of actions (unused)
        }


    Locations
    ---------

      `source` and `target` locations can be given as strings or `ru.URL`
      instances.  Strings containing `://` are converted into URLs immediately.
      Otherwise they are considered absolute or relative paths and are then
      interpreted in the context of the client's working directory.

      RP accepts the following special URL schemas:

        * `client://`  : relative to the client's working directory
        * `resource://`: relative to the RP    sandbox on the target resource
        * `pilot://`   : relative to the pilot sandbox on the target resource
        * `unit://`    : relative to the unit  sandbox on the target resource

      In all these cases, the `hostname` element of the URL is expected to be
      empty, and the path is *always* considered relative to the locations
      specified above (even though URLs usually don't have a notion of relative
      paths).


    Action operators
    ----------------

      RP accepts the following action operators:

        * rp.TRANSFER: remote file transfer from `source` URL to `target` URL.
        * rp.COPY    : local file copy, ie. not crossing host boundaries
        * rp.MOVE    : local file move
        * rp.LINK    : local file symlink


    Flags
    -----

      rp.CREATE_PARENTS: create the directory hierarchy for targets on the fly
      rp.RECURSIVE     : if `source` is a directory, handle it recursively

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        if from_dict:
            for k,v in from_dict.iteritems():
                self[k] = v



    # --------------------------------------------------------------------------
    #
    def verify(self):
        '''
        Verify that the description is syntactically and semantically correct.
        This method encapsulates checks beyond the SAGA attribute level checks.
        '''

        # replace 'None' values for string types with '', for int types with '0'
        self.setdefault(KERNEL          , '')
        self.setdefault(NAME            , '')
        self.setdefault(EXECUTABLE      , '')
        self.setdefault(ARGUMENTS       , '')
        self.setdefault(ENVIRONMENT     , '')
        self.setdefault(PRE_EXEC        , '')
        self.setdefault(POST_EXEC       , '')
        self.setdefault(PILOT           , '')
        self.setdefault(STDOUT          , '')
        self.setdefault(STDERR          , '')
        self.setdefault(CPU_PROCESS_TYPE, '')
        self.setdefault(CPU_THREAD_TYPE , '')
        self.setdefault(GPU_PROCESS_TYPE, '')
        self.setdefault(GPU_THREAD_TYPE , '')

        self.setdefault(CPU_PROCESSES   , 0)
        self.setdefault(CPU_THREADS     , 0)
        self.setdefault(GPU_PROCESSES   , 0)
        self.setdefault(GPU_THREADS     , 0)
        self.setdefault(MEM_PER_PROCESS , 0)

        if  not self.get('executable') and \
            not self.get('kernel')     :
            raise ValueError("CU description needs 'executable' or 'kernel'")


# ------------------------------------------------------------------------------

