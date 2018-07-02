
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes as attributes


# ------------------------------------------------------------------------------
# Attribute description keys
#
RESOURCE          = 'resource'
ACCESS_SCHEMA     = 'access_schema'
QUEUE             = 'queue'
PROJECT           = 'project'
CANDIDATE_HOSTS   = 'candidate_hosts'
SANDBOX           = 'sandbox'
OUTPUT            = 'output'
ERROR             = 'error'
RUNTIME           = 'runtime'
CLEANUP           = 'cleanup'
EXIT_ON_ERROR     = 'exit_on_error'
_CONFIG           = '_config'

CORES             = 'cores'
GPUS              = 'gpus'
MEMORY            = 'memory'


# ------------------------------------------------------------------------------
#
class ComputePilotDescription(attributes.Attributes):
    """
    A ComputePilotDescription object describes the requirements and properties
    of a :class:`radical.pilot.Pilot` and is passed as a parameter to
    :meth:`radical.pilot.PilotManager.submit_pilots` to instantiate and run
    a new pilot.

    .. note:: A ComputePilotDescription **MUST** define at least
              :data:`resource`, :data:`cores` and :data:`runtime`.

    **Example**::

          pm = radical.pilot.PilotManager(session=s)

          pd = radical.pilot.ComputePilotDescription()
          pd.resource = "local.localhost"  # defined in futuregrid.json
          pd.cores    = 16
          pd.runtime  = 5 # minutes

          pilot = pm.submit_pilots(pd)

    .. data:: resource

       [Type: `string`] [**`mandatory`**] The key of a
       :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the
       configuration once the ComputePilotDescription is passed to
       :meth:`radical.pilot.PilotManager.submit_pilots`. If the key doesn't exist,
       a :class:`radical.pilot.pilotException` is thrown.

    .. data:: access_schema

       [Type: `string`] [**`optional`**] The key of an access mechanism to use.
       The valid access mechanism are defined in the resource configurations,
       see :ref:`chapter_machconf`.  The first one defined there is used by
       default, if no other is specified.

    .. data:: runtime

       [Type: `int`] [**mandatory**] The maximum run time (wall-clock time) in
       **minutes** of the ComputePilot.

    .. data:: sandbox

       [Type: `string`] [optional] The working ("sandbox") directory  of the
       ComputePilot agent. This parameter is optional. If not set, it defaults
       to `radical.pilot.sandox` in your home or login directory.

       .. warning:: If you define a ComputePilot on an HPC cluster and you want
                 to set `sandbox` manually, make sure that it points to a
                 directory on a shared filesystem that can be reached from all
                 compute nodes.

    .. data:: cores

       [Type: `int`] [**mandatory**] The number of cores the pilot should
       allocate on the target resource.

       NOTE: for local pilots, you can set a number larger than the physical
       machine limit when setting `RADICAL_PILOT_PROFILE` in your environment.

    .. data:: memory

       [Type: `int`] [**optional**] The amount of memorty (in MB) the pilot
       should allocate on the target resource.

    .. data:: queue

       [Type: `string`] [optional] The name of the job queue the pilot should
       get submitted to . If `queue` is defined in the resource configuration
       (:data:`resource`) defining `queue` will override it explicitly.

    .. data:: project

       [Type: `string`] [optional] The name of the project / allocation to
       charge for used CPU time. If `project` is defined in the machine
       configuration (:data:`resource`), defining `project` will
       override it explicitly.

    .. data:: candidate_hosts

       [Type: `list`] [optional] The list of names of hosts where this pilot
       is allowed to start on.

    .. data:: cleanup

       [Type: `bool`] [optional] If cleanup is set to True, the pilot will 
       delete its entire sandbox upon termination. This includes individual
       ComputeUnit sandboxes and all generated output data. Only log files will 
       remain in the sandbox directory. 

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register    (RESOURCE,         None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (ACCESS_SCHEMA,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (RUNTIME,          None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (SANDBOX,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (CORES,            None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (GPUS,             None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (MEMORY,           None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (QUEUE,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (PROJECT,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (CLEANUP,          None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (CANDIDATE_HOSTS,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register    (EXIT_ON_ERROR,    None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)

        # Attributes not part of the published API
        self._attributes_register    (_CONFIG,          None, attributes.ANY,    attributes.SCALAR, attributes.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (RESOURCE,         None)
        self.set_attribute (ACCESS_SCHEMA,    None)
        self.set_attribute (RUNTIME,          None)
        self.set_attribute (SANDBOX,          None)
        self.set_attribute (CORES,               0)
        self.set_attribute (GPUS,                0)
        self.set_attribute (MEMORY,           None)
        self.set_attribute (QUEUE,            None)
        self.set_attribute (PROJECT,          None)
        self.set_attribute (CLEANUP,          None)
        self.set_attribute (CANDIDATE_HOSTS,  None)
        self.set_attribute (EXIT_ON_ERROR,    False)
        self.set_attribute (_CONFIG,          None)

        # apply initialization dict
        if from_dict:
            self.from_dict(from_dict)

      # FIXME
      #     logger.report.plain('[%s:%s:%s]' % (from_dict.get(RESOURCE, ''), 
      #                                         from_dict.get(CORES,     1), 
      #                                         from_dict.get(GPUS,      0)))
      # logger.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):

        other = ComputePilotDescription ()

        for key in self.list_attributes ():
            other.set_attribute(key, self.get_attribute (key))

        return other


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


# ------------------------------------------------------------------------------

