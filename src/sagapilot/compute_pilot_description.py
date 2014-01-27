"""
.. module:: sagapilot.compute_pilot_description
   :platform: Unix
   :synopsis: Implementation of the ComputePilotDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sagapilot.attributes  as attributes

# ------------------------------------------------------------------------------
# Attribute description keys
RESOURCE          = 'Resource'
QUEUE             = 'Queue'
CORES             = 'Cores'
WORKING_DIRECTORY = 'WorkingDirectory'
OUTPUT            = 'Output'
ERROR             = 'Error'
RUN_TIME          = 'RunTime'
CLEANUP           = 'Cleanup'
PROJECT           = 'Project'

# ------------------------------------------------------------------------------
#
class ComputePilotDescription (attributes.Attributes) :
    """A ComputePilotDescription object describes the requirements and 
    properties of a :class:`sagapilot.Pilot` and is passed as a parameter to
    :meth:`sagapilot.PilotManager.submit_pilots` to instantiate a new pilot.

    .. note:: A ComputePilotDescription **MUST** define at least 
              :data:`resource` and the number of :data:`cores` to allocate on 
              the target resource.

    **Example**::

        pm = sagapilot.PilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

        pd = sagapilot.ComputePilotDescription()
        pd.resource = "futuregrid.INDIA"  # Key defined in futuregrid.json
        pd.cores = 16

        pilot_india = pm.submit_pilots(pd)

    .. data:: resource 

       (`Property`) [Type: `string` or `list of strings`] 
       The key of a :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the 
       configuration once the ComputePilotDescription is passed to 
       :meth:`sagapilot.PilotManager.submit_pilots`. If the key doesn't exist, a
       :class:`sagapilot.SagapilotException` is thrown.

    .. data:: cores

       (`Property`) [Type: `int`] The number of cores the pilot should allocate 
       on the target resource.

    .. data:: queue

       (`Property`) [Type: `string`] The name of the job queue the pilot should 
       get submitted to . If `queue` is defined in the resource configuration 
       (:data:`resource`) defining `queue` will override it explicitly. 

    .. data:: allocation 

       (`Property`) [Type: `string`] The name of the project / allocation to 
       charge for used CPU time. If `allocation` is defined in the machine 
       configuration (:data:`resource`), defining `allocation` will 
       override it explicitly.


    .. data:: run_time  

       (`Property`) [Type: `int`] The total run time (wall-clock time) in 
       **minutes** of the pilot.

    """

    def __init__ (self, vals={}) : 
        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # runtime properties
        self._attributes_register(RUN_TIME,          None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        #self._attributes_register(RUN_TIME,                    None, attributes.TIME,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(CLEANUP,           None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PROJECT,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        # i/o
        self._attributes_register(WORKING_DIRECTORY, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(OUTPUT,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ERROR,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(FILE_TRANSFER,    None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)

        # resource requirements
        self._attributes_register(RESOURCE,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(CORES,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(CANDIDATE_HOSTS,  None, attributes.INT,    attributes.VECTOR, attributes.WRITEABLE)
        #self._attributes_register(CPU_ARCHITECTURE, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(OPERATING_SYSTEM, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        #self._attributes_register(MEMORY,           None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(QUEUE,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

    #------------------------------------------------------------------------------
    #
    def as_dict(self):
      """Returns dict/JSON representation.
      """
      return attributes.Attributes.as_dict(self)

    # ------------------------------------------------------------------------------
    #
    def __str__(self):
      """Returns string representation.
      """
      return str(self.as_dict())

