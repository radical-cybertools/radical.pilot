#pylint: disable=C0301, C0103, W0212, E1101, R0903

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
INPUT_DATA             = 'input_data'
OUTPUT_DATA            = 'output_data'

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

    .. data:: name 

       (`Attribute`) A descriptive name for the compute unit (`string`) [`optional`].

    .. data:: arguments 

       (`Attribute`) The arguments for :data:`executable` (`list` of `strings`) [`optional`].

    .. data:: environment 

       (`Attribute`) Environment variables to set in the execution environment (`dict`) [`optional`].

    .. data:: input_data 

       (`Attribute`) The input files that need to be transferred before execution (`transfer directive string`) [`optional`].

       .. note:: TODO: Explain transfer directives.

    .. data:: output_data 

       (`Attribute`) The output files that need to be transferred back after execution (`transfer directive string`) [`optional`].

       .. note:: TODO: Explain transfer directives.

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
        self._attributes_register(NAME,                   None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(EXECUTABLE,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ARGUMENTS,              None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(ENVIRONMENT,            None, attributes.STRING, attributes.DICT,   attributes.WRITEABLE)
        #self._attributes_register(CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        self._attributes_register(INPUT_DATA,             None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(OUTPUT_DATA,            None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register(CORES,                  None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
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
        # Apparently the aatribute interface only handles 'non-None' attributes,
        # so we do it manually. More explicit anyways.
        obj_dict = {
            NAME                   : self.name,
            EXECUTABLE             : self.executable,
            ARGUMENTS              : self.arguments,
            ENVIRONMENT            : self.environment,
            CORES                  : self.cores,
            INPUT_DATA             : self.input_data, 
            OUTPUT_DATA            : self.output_data
        }
        return obj_dict

    #------------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())
