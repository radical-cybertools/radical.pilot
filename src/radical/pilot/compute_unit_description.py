# pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the ComputeUnitDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

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
STDOUT                 = 'stdout'
STDERR                 = 'stderr'
RESTARTABLE            = 'restartable'

# ------------------------------------------------------------------------------
#
class ComputeUnitDescription(attributes.Attributes) :
    """A ComputeUnitDescription object describes the requirements and 
    properties of a :class:`radical.pilot.ComputeUnit` and is passed as a parameter to
    :meth:`radical.pilot.UnitManager.submit_units` to instantiate and run a new 
    ComputeUnit.

    .. note:: A ComputeUnitDescription **MUST** define at least an :data:`executable`.

    **Example**::

        # TODO 

    .. data:: executable 

       (`Attribute`) The executable to launch (`string`) [`mandatory`].

    .. data:: cores 

       (`Attribute`) The number of cores (int) required by the executable. (int) [`mandatory`].

    .. data:: mpi

       (`Attribute`) Set to true if the task is an MPI task. (bool) [`optional`].

    .. data:: name 

       (`Attribute`) A descriptive name for the compute unit (`string`) [`optional`].

    .. data:: arguments 

       (`Attribute`) The arguments for :data:`executable` (`list` of `strings`) [`optional`].

    .. data:: environment 

       (`Attribute`) Environment variables to set in the execution environment (`dict`) [`optional`].

    .. data:: stdout

       (`Attribute`) the name of the file to store stdout in.

    .. data:: stderr

       (`Attribute`) the name of the file to store stderr in.

    .. data:: input_staging

       (`Attribute`) The files that need to be staged before execution (`list` of `staging directives`) [`optional`].

       .. note:: TODO: Explain input staging.

    .. data:: output_staging

       (`Attribute`) The files that need to be staged after execution (`list` of `staging directives`) [`optional`].

       .. note:: TODO: Explain output staging.

    .. data:: pre_exec

       (`Attribute`) Actions to perform before this task starts (`list` of `strings`) [`optional`].

    .. data:: post_exec

       (`Attribute`) Actions to perform after this task finishes (`list` of `strings`) [`optional`].

       .. note:: Before the BigBang, there was nothing ...

    .. data:: kernel

       (`Attribute`) Name of a simulation kernel which expands to description
       attributes once the unit is scheduled to a pilot (and resource).

       .. note:: TODO: explain in detal, reference ENMDTK.

    .. data:: restartable

       (`Attribute`) If the unit starts to execute on a pilot, but cannot finish
       because the pilot fails or is canceled, can the unit be restarted on
       a different pilot / resource? (default: False)

       .. note:: TODO: explain in detal, reference ENMDTK.

    .. data:: cleanup

       [Type: `bool`] [optional] If cleanup is set to True, the pilot will
       delete the entire unit sandbox upon termination. This includes all
       generated output data in that sandbox.  Output staging will be performed
       before cleanup.

    """
    def __init__(self, from_dict=None):
        """Le constructeur.
        """ 

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

      # disabled deprecated attributes
      # self._attributes_register_deprecated ('input_data',  'input_staging',  flow=self._DOWN)
      # self._attributes_register_deprecated ('output_data', 'output_staging', flow=self._DOWN)

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

        # apply initialization dict
        if from_dict:
            self.from_dict(from_dict)


    #------------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):

        other = ComputeUnitDescription ()

        for key in self.list_attributes () :
            other.set_attribute (key, self.get_attribute (key))

        return other


# ---------------------------------------------------------------------------------

