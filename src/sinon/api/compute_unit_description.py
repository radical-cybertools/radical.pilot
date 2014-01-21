"""
.. module:: sinon.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the ComputeUnitDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sinon.api.types       as types
import sinon.api.attributes  as attributes

# ------------------------------------------------------------------------------
# Attribute description keys
NAME              = 'Name'
EXECUTABLE        = 'Executable'
ARGUMENTS         = 'Arguments'
ENVIRONMENT       = 'Environment'
CORES             = 'Cores'

# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (attributes.Attributes) :
    """A ComputeUnitDescription object describes the requirements and 
    properties of a :class:`sinon.ComputeUnit` and is passed as a parameter to
    :meth:`sinon.UnitManager.submit_units` to instantiate and run a new 
    ComputeUnit.

    .. note:: A ComputeUnitDescription **MUST** define at least an :data:`executable`.

    **Example**::

        # TODO 

    .. data:: name 

       (`Attribute`) The name of the compute unit (`string`) [`optional`].

    .. data:: executable 

       (`Attribute`) The executable to launch (`string`) [`mandatory`].

    .. data:: arguments 

       (`Attribute`) The arguments for :data:`executable` (`list` of `strings`) [`optional`].

    .. data:: environment 

       (`Attribute`) Environment variables to set in the execution environment (`dict`) [`optional`].

    """
    def __init__ (self, vals={}) : 

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(NAME,              None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(EXECUTABLE,        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ARGUMENTS,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(ENVIRONMENT,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        #self._attributes_register(WORKING_DIRECTORY, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(INPUT,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(OUTPUT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(ERROR,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(FILE_TRANSFER,     None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(INPUT_DATA,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(OUTPUT_DATA,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # parallelism
        #self._attributes_register(SPMD_VARIATION,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register(CORES,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
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
      """Returns dict/JSON representation.
      """
      return attributes.Attributes.as_dict(self)

    #------------------------------------------------------------------------------
    #
    def __str__(self):
      """Returns string representation.
      """
      return str(self.as_dict())
