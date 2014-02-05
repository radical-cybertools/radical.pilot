#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: sagapilot.compute_pilot_description
   :platform: Unix
   :synopsis: Implementation of the ComputePilotDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sagapilot.attributes as attributes

# ------------------------------------------------------------------------------
# Attribute description keys
RESOURCE          = 'Resource'
QUEUE             = 'Queue'
CORES             = 'Cores'
WORKING_DIRECTORY = 'WorkingDirectory'
SANDBOX           = 'Sandbox'
OUTPUT            = 'Output'
ERROR             = 'Error'
RUN_TIME          = 'RunTime'
RUNTIME           = 'Runtime'
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
        pd.resource = "localhost"
        pd.sandbox  = "/tmp/sagapilot.sandbox"
        pd.runtime  = 10
        pd.cores    = 16

        pilot_india = pm.submit_pilots(pd)

    .. data:: resource 

       (`Property`) [Type: `string` or `list of strings`] 
       The key of a :ref:`chapter_machconf` entry. [`mandatory`]
       If the key exists, the machine-specifc configuration is loaded from the 
       configuration once the ComputePilotDescription is passed to 
       :meth:`sagapilot.PilotManager.submit_pilots`. If the key doesn't exist, a
       :class:`sagapilot.SagapilotException` is thrown.

    .. data:: sandbox 

       (`Property`) [Type: `string`]  The working direcotry ("sandbox") of the 
       ComputePilot agent. [`mandatory`]

    .. data:: runtime  

       (`Property`) [Type: `int`] The total run time (wall-clock time) in 
       **minutes** of the ComputePilot. [`mandatory`]

    .. data:: cores

       (`Property`) [Type: `int`] The number of cores the pilot should allocate 
       on the target resource. [`mandatory`]

    .. data:: queue

       (`Property`) [Type: `string`] The name of the job queue the pilot should 
       get submitted to . If `queue` is defined in the resource configuration 
       (:data:`resource`) defining `queue` will override it explicitly. [`optional`]

    .. data:: allocation 

       (`Property`) [Type: `string`] The name of the project / allocation to 
       charge for used CPU time. If `allocation` is defined in the machine 
       configuration (:data:`resource`), defining `allocation` will 
       override it explicitly. [`optional`]

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructeur.
        """ 
        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register            (RUNTIME, None, attributes.INT, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register_deprecated (RUN_TIME, RUNTIME, flow='_down')

        self._attributes_register            (CLEANUP, None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register            (PROJECT, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        self._attributes_register            (SANDBOX, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register_deprecated (WORKING_DIRECTORY, SANDBOX, flow='_down')

        self._attributes_register             (RESOURCE, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register             (CORES, None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register             (QUEUE, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

    #------------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        # Apparently the atribute interface only handles 'non-None' attributes,
        # so we do some manual check-and-set.
        d =  attributes.Attributes.as_dict(self)

        if RUNTIME not in d:
            d[RUNTIME] = None

        if CLEANUP not in d:
            d[CLEANUP] = None

        if PROJECT not in d:
            d[PROJECT] = None

        if SANDBOX not in d:
            d[SANDBOX] = None

        if RESOURCE not in d:
            d[RESOURCE] = None

        if CORES not in d:
            d[CORES] = None

        if QUEUE not in d:
            d[QUEUE] = None

        return d

    # ------------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())

