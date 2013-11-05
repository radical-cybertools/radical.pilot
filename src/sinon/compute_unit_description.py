"""
.. module:: sinon.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the ComputeUnitDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

import constants
import attributes
import description

# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (description.Description) :
    """A ComputeUnitDescription object describes the requirements and 
    properties of a :class:`sinon.ComputeUnit` and is passed as a parameter to
    :meth:`sinon.UnitManager.submit_units` to instantiate and run a new 
    ComputeUnit.

    A ComputeUnitDescription **must** define at least a :data:`executable`.

    **Example**::

        # TODO 

    .. data:: executable 

       (`Property`) The executable to launch.
    """
    def __init__ (self, vals={}) : 

        descr.Description.__init__ (self, vals)

        self._attributes_i_set  ('dtype', constants.COMPUTE, self._DOWN)

        # register properties with the attribute interface
        # action description
        self._attributes_register  (constants.NAME,              None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.EXECUTABLE,        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.ARGUMENTS,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.ENVIRONMENT,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        # I/O
        self._attributes_register  (constants.WORKING_DIRECTORY, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.INPUT,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.OUTPUT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.ERROR,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.FILE_TRANSFER,     None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.INPUT_DATA,        None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.OUTPUT_DATA,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # parallelism
        self._attributes_register  (constants.SPMD_VARIATION,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.SLOTS,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register  (constants.CPU_ARCHITECTURE,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.OPERATING_SYSTEM,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.MEMORY,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        # dependencies
        self._attributes_register  (constants.RUN_AFTER,         None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.START_AFTER,       None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register  (constants.CONCURRENT_WITH,   None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)


    def as_dict(self):
        """Returns the object as JSON string.

            **Returns:**
                * A JSON `string` representing the ComputeUnitDescription.
        """
        return description.Description.as_dict(self)
