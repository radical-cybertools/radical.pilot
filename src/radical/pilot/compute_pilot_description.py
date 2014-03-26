#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.compute_pilot_description
   :platform: Unix
   :synopsis: Provides the interface for the ComputePilotDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes  as attributes


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
#
class ComputePilotDescription(attributes.Attributes):
    """A ComputePilotDescription object describes the requirements and
    properties of a :class:`radical.pilot.Pilot` and is passed as a parameter to
    :meth:`radical.pilot.PilotManager.submit_pilots` to instantiate a new pilot.

    .. note:: A ComputePilotDescription **MUST** define at least
              :data:`resource` and the number of :data:`cores` to allocate on
              the target resource.

    **Example**::

          pm = radical.pilot.PilotManager(session=s)

          pd = radical.pilot.ComputePilotDescription()
          pd.resource = "localhost"  # defined in futuregrid.json
          pd.cores    = 16
          pd.runtime  = 5 # minutes

          pilot = pm.submit_pilots(pd)

    .. data:: resource

       [Type: `string` or `list of strings`] [**`mandatory`**] The key of a
       :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the
       configuration once the ComputePilotDescription is passed to
       :meth:`radical.pilot.PilotManager.submit_pilots`. If the key doesn't exist,
       a :class:`radical.pilot.radical.pilotException` is thrown.

    .. data:: runtime

       [Type: `int`] [**mandatory**] The total run time (wall-clock time) in
       **minutes** of the ComputePilot.

    .. data:: cores

       [Type: `int`] [**mandatory**] The number of cores the pilot should
       allocate on the target resource.

    .. data:: sandbox

       [Type: `string`] [optional] The working ("sandbox") directory  of the
       ComputePilot agent. This parameter is optional. If not set, it defaults
       to `radical.pilot.sandox` in your home or login directory.

       .. warning:: If you define a ComputePilot on an HPC cluster and you want
                 to set `sandbox` manually, make sure that it points to a
                 directory on a shared filesystem that can be reached from all
                 compute nodes.

    .. data:: queue

       [Type: `string`] [optional] The name of the job queue the pilot should
       get submitted to . If `queue` is defined in the resource configuration
       (:data:`resource`) defining `queue` will override it explicitly.

    .. data:: project

       [Type: `string`] [optional] The name of the project / allocation to
       charge for used CPU time. If `project` is defined in the machine
       configuration (:data:`resource`), defining `project` will
       override it explicitly.

    .. data:: cleanup

       [Type: `bool`] [optional] If cleanup is set to True, the pilot will 
       delete its entire sandbox upon termination. This includes individual
       ComputeUnit sandboxes and all generated output data. Only log files will 
       remain in the sandbox directory. 

    """

    # -------------------------------------------------------------------------
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

    #--------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        # Apparently the atribute interface only handles 'non-None' attributes,
        # so we do some manual check-and-set.
        d = attributes.Attributes.as_dict(self)

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

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())
