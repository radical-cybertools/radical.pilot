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

    """
    def __init__(self):
        """Le constructeur.
        """ 

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(KERNEL,                 None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(NAME,                   None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(EXECUTABLE,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ARGUMENTS,              None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(ENVIRONMENT,            None, attributes.STRING, attributes.DICT,   attributes.WRITEABLE)
        self._attributes_register(PRE_EXEC,               None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(POST_EXEC,              None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        self._attributes_register(INPUT_STAGING,          None, attributes.ANY, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(OUTPUT_STAGING,         None, attributes.ANY, attributes.VECTOR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register(CORES,                  1, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(MPI,                    False, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(CPU_ARCHITECTURE,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(OPERATING_SYSTEM,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(MEMORY,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # dependencies
        #self._attributes_register(RUN_AFTER,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(START_AFTER,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(CONCURRENT_WITH,   None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)


    #------------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        # Apparently the attribute interface only handles 'non-None' attributes,
        # so we do it manually. More explicit anyways.
        obj_dict = {
            KERNEL                 : self.kernel,
            NAME                   : self.name,
            EXECUTABLE             : self.executable,
            ARGUMENTS              : self.arguments,
            ENVIRONMENT            : self.environment,
            CORES                  : self.cores,
            MPI                    : self.mpi,
            PRE_EXEC               : self.pre_exec,
            POST_EXEC              : self.post_exec
        }
        if not self.input_staging:
            obj_dict[INPUT_STAGING] = []
        elif not isinstance(self.input_staging, list):
            obj_dict[INPUT_STAGING] = [self.input_staging.as_dict()]
        else:
            obj_dict[INPUT_STAGING] = [x.as_dict() for x in self.input_staging]

        if not self.output_staging:
            obj_dict[OUTPUT_STAGING] = []
        elif not isinstance(self.output_staging, list):
            obj_dict[OUTPUT_STAGING] = [self.output_staging.as_dict()]
        else:
            obj_dict[OUTPUT_STAGING] = [x.as_dict() for x in self.output_staging]

        return obj_dict


    #------------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


    #------------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):
        """Returns a string representation of the object.
        """

        other = ComputeUnitDescription ()

        d = self.as_dict ()
        for key in d :
            self[key] = d[key]

        return other

