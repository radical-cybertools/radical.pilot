"""
.. module:: sinon.compute_pilot_description
   :platform: Unix
   :synopsis: Implementation of the ComputePilotDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

import constants
import attributes
import description

# ------------------------------------------------------------------------------
#
class ComputePilotDescription (description.Description) :
    """A ComputePilotDescription object describes the requirements and 
    properties of a :class:`sinon.Pilot` and is passed as a parameter to
    :meth:`sinon.PilotManager.submit_pilots` to instantiate a new pilot.

    A ComputePilotDescription **must** define at least a :data:`resource` key
    and number of :data:`cores` to allocate on the target resource.

    **Example**::

        pm = sinon.PilotManager(session=s, machine_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

        pd = sinon.ComputePilotDescription()
        pd.resource = "futuregrid.INDIA"  # Key defined in futuregrid.json
        pd.cores = 16

        pilot_india = pm.submit_pilots(pd)

    .. data:: resource 

       (`Property`) The key of a :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the 
       configuration once the ComputePilotDescription is passed to 
       :meth:`sinon.PilotManager.submit_pilots`. If the key doesn't exist, a
       :class:`sinon.SinonException` is thrown.

    .. data:: cores 

       (`Property`) The number of cores the pilot should allocate 
       on the target resource.

    .. data:: queue 

       (`Property`) The name of the job queue the pilot should get submitted to .
       If `queue` is defined in the machine configuration (:data:`resource`), 
       setting `queue` explicitly will override it.

    .. data:: allocation 

       (`Property`) The name of the project / allocation to charge for used CPU time.
       If `allocation` is defined in the machine configuration (:data:`resource`), 
       setting `allocation` explicitly will override it.


    .. data:: run_time 

       (`Property`) The total run time (wall-clock time) of the pilot.

    """

    def __init__ (self, vals={}) : 

        description.Description.__init__ (self, vals)

        # register properties with the attribute interface
        # runtime properties
        self._attributes_register  (constants.START_TIME,        None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)

        #self._attributes_register  (constants.RUN_TIME,          None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.ALLOCATION,        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        # i/o
        self._attributes_register  (constants.WORKING_DIRECTORY, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.OUTPUT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.ERROR,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.FILE_TRANSFER,     None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register  (constants.RESOURCE,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.CORES,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.SPMD_VARIATION,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CANDIDATE_HOSTS,   None, attributes.INT,    attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register  (constants.CPU_ARCHITECTURE,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.OPERATING_SYSTEM,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register  (constants.MEMORY,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register  (constants.QUEUE,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

    def as_dict(self):
        """Returns the object as JSON string.

            **Returns:**
                * A JSON `string` representing the ComputePilotDescription.
        """
        return description.Description.as_dict(self)
