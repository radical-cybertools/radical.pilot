"""
.. module:: sinon.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the ComputeUnitDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

from sinon.constants import *
import sinon.frontend.attributes  as attributes
import sinon.frontend.description as description

# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (description.Description) :
    """A ComputeUnitDescription object describes the requirements and 
    properties of a :class:`sinon.ComputeUnit` and is passed as a parameter to
    :meth:`sinon.UnitManager.submit_units` to instantiate and run a new 
    ComputeUnit.

    .. note:: A ComputeUnitDescription **MUST** define at least an :data:`executable`.

    **Example**::

        # TODO 

    .. data:: executable 

       (`Property`) The executable to launch (`string`).

    .. data:: arguments 

       (`Property`) The arguments for :data:`executable` (`list` of `strings`).

    .. data:: environment 

       (`Property`) Environment variables to set in the execution environment (`dict`).

    """
    def __init__ (self, vals={}) : 

        description.Description.__init__ (self, vals)

        self._attributes_i_set  ('dtype', COMPUTE, self._DOWN)

        # register properties with the attribute interface
        # action description
        #self._attributes_register  (constants.NAME,              None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (EXECUTABLE,        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (ARGUMENTS,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (ENVIRONMENT,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        #self._attributes_register  (constants.WORKING_DIRECTORY, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.INPUT,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.OUTPUT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.ERROR,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.FILE_TRANSFER,     None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.INPUT_DATA,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.OUTPUT_DATA,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # parallelism
        #self._attributes_register  (constants.SPMD_VARIATION,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CORES,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # resource requirements
        #self._attributes_register  (constants.CPU_ARCHITECTURE,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.OPERATING_SYSTEM,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.MEMORY,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # dependencies
        #self._attributes_register  (constants.RUN_AFTER,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.START_AFTER,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CONCURRENT_WITH,   None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)


    def as_dict(self):
        """Returns the object as JSON string.

            **Returns:**
                * A JSON `string` representing the ComputeUnitDescription.
        """
        return description.Description.as_dict(self)
