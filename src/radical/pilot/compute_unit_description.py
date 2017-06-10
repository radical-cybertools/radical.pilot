
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes as attributes


# ------------------------------------------------------------------------------
# Attribute description keys
NAME                   = 'name'
EXECUTABLE             = 'executable'
ARGUMENTS              = 'arguments'
ENVIRONMENT            = 'environment'
CORES                  = 'cores'
INPUT_STAGING          = 'input_staging'
OUTPUT_STAGING         = 'output_staging'
MPI                    = 'mpi'
PRE_EXEC               = 'pre_exec'
POST_EXEC              = 'post_exec'
KERNEL                 = 'kernel'
CLEANUP                = 'cleanup'
PILOT                  = 'pilot'
STDOUT                 = 'stdout'
STDERR                 = 'stderr'
RESTARTABLE            = 'restartable'

# ------------------------------------------------------------------------------
#
class ComputeUnitDescription(attributes.Attributes):
    """
    A ComputeUnitDescription object describes the requirements and properties
    of a :class:`radical.pilot.ComputeUnit` and is passed as a parameter to
    :meth:`radical.pilot.UnitManager.submit_units` to instantiate and run
    a new unit.

    .. note:: A ComputeUnitDescription **MUST** define at least an
              `executable` or `kernel` -- all other elements are optional.

    **Example**::

        # TODO 

    .. data:: executable 

       The executable to launch (`string`).  The executable is expected to be
       either available via `$PATH` on the target resource, or to be an absolute
       path.

       default: `None`


    .. data:: cores 

       The number of cores required by the executable. (int).  RP will not
       control how the executable makes use of the assigned cores, it only
       ensures that those are available to the executable (but see `mpi` below).

       default: `1`


    .. data:: mpi

       Set to true if the task is an MPI task (bool).  If this is set, RP will
       ensure that the executable is run via mpirun or equivalent.  What MPI is
       used to start the executable depends on the pilot configuration, and
       cannot currently be switched on a per-unit basis.

       default: `False`


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

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(KERNEL,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(NAME,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(EXECUTABLE,       None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ARGUMENTS,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(ENVIRONMENT,      None, attributes.STRING, attributes.DICT,   attributes.WRITEABLE)
        self._attributes_register(PRE_EXEC,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(POST_EXEC,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(RESTARTABLE,      None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(CLEANUP,          None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

      # self._attributes_register(START_TIME,       None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
      # self._attributes_register(RUN_TIME,         None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        self._attributes_register(STDOUT,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(STDERR,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(INPUT_STAGING,    None, attributes.ANY,    attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(OUTPUT_STAGING,   None, attributes.ANY,    attributes.VECTOR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register(CORES,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(MPI,              None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
      # self._attributes_register(CPU_ARCHITECTURE, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
      # self._attributes_register(OPERATING_SYSTEM, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
      # self._attributes_register(MEMORY,           None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # dependencies
      # self._attributes_register(RUN_AFTER,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
      # self._attributes_register(START_AFTER,      None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
      # self._attributes_register(CONCURRENT_WITH,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (KERNEL,         None)
        self.set_attribute (NAME,           None)
        self.set_attribute (EXECUTABLE,     None)
        self.set_attribute (ARGUMENTS,      None)
        self.set_attribute (ENVIRONMENT,    None)
        self.set_attribute (PRE_EXEC,       None)
        self.set_attribute (POST_EXEC,      None)
        self.set_attribute (STDOUT,         None)
        self.set_attribute (STDERR,         None)
        self.set_attribute (INPUT_STAGING,  None)
        self.set_attribute (OUTPUT_STAGING, None)
        self.set_attribute (CORES,             1)
        self.set_attribute (MPI,           False)
        self.set_attribute (RESTARTABLE,   False)
        self.set_attribute (CLEANUP,       False)
        self.set_attribute (PILOT,          None)

        # apply initialization dict
        if from_dict:
            self.from_dict(from_dict)


    #---------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):

        other = ComputeUnitDescription ()

        for key in self.list_attributes ():
            other.set_attribute(key, self.get_attribute (key))

        return other


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


# ------------------------------------------------------------------------------

