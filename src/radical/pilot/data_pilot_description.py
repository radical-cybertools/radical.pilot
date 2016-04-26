
"""
.. module:: radical.pilot.data_pilot_description
   :platform: Unix
   :synopsis: Provides the interface for the DataPilotDescription class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes  as attributes


# -----------------------------------------------------------------------------
# Attribute description keys
RESOURCE          = 'resource'
ACCESS_SCHEMA     = 'access_schema'
SIZE              = 'size'
SANDBOX           = 'sandbox'
OUTPUT            = 'output'
ERROR             = 'error'
RUNTIME           = 'runtime'
CLEANUP           = 'cleanup'
_CONFIG           = '_config'


# -----------------------------------------------------------------------------
#
class DataPilotDescription(attributes.Attributes):
    """
    A DataPilotDescription object describes the requirements and properties of
    a :class:`radical.pilot.DataPilot` and is passed as a parameter to
    :meth:`radical.pilot.PilotManager.submit_pilots` to instantiate a new data
    pilot.

    .. note:: A DataPilotDescription **MUST** define at least
              :data:`resource`.

    **Example**::

          pm = radical.pilot.PilotManager(session=s)

          pd = radical.pilot.DataPilotDescription()
          pd.resource = "local.localhost"
          pd.size     = 16 # MegaByte
          pd.runtime  =  5 # minutes

          pilot = pm.submit_pilots(pd)

    .. data:: resource

       [Type: `string`] [**`mandatory`**] The key of a
       :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the
       configuration once the DataPilotDescription is passed to
       :meth:`radical.pilot.PilotManager.submit_pilots`. If the key doesn't exist,
       a :class:`radical.pilot.pilotException` is thrown.

    .. data:: access_schema

       [Type: `string`] [**`optional`**] The key of an access mechanism to use.
       The valid access mechanism are defined in the resource configurations,
       see :ref:`chapter_machconf`.  The first one defined there is used by
       default, if no other is specified.

    .. data:: runtime

       [Type: `int`] [**mandatory**] The maximum run time (wall-clock time) in
       **minutes** of the DataPilot.

    .. data:: sandbox

       [Type: `string`] [optional] The working ("sandbox") directory  of the
       DataPilot agent. This parameter is optional. If not set, it defaults
       to `radical.pilot.sandox` in your home or login directory.

       .. warning:: If you define a DataPilot on an HPC cluster and you want
                 to set `sandbox` manually, make sure that it points to a
                 directory on a shared filesystem that can be reached from all
                 data nodes.

    .. data:: size

       [Type: `int`] [optional] The number of megabytes the pilot should
       allocate on the target resource.

       NOTE: for local pilots, you can set a number larger than the physical
       machine limit when setting `RADICAL_PILOT_PROFILE` in your environment.

    .. data:: cleanup

       [Type: `bool`] [optional] If cleanup is set to True, the pilot will 
       delete its entire sandbox upon termination. This includes individual
       DataUnit sandboxes.

    """

    # -------------------------------------------------------------------------
    #
    def __init__(self):

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register    (RESOURCE     , None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (ACCESS_SCHEMA, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (SIZE         , None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (SANDBOX      , None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (OUTPUT       , None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (ERROR        , None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (RUNTIME      , None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register    (CLEANUP      , None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)

        # Attributes not part of the published API
        self._attributes_register    (_CONFIG      , None, attributes.ANY,    attributes.SCALAR, attributes.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (RESOURCE,         None)
        self.set_attribute (ACCESS_SCHEMA,    None)
        self.set_attribute (RUNTIME,          None)
        self.set_attribute (SANDBOX,          None)
        self.set_attribute (CORES,            None)
        self.set_attribute (MEMORY,           None)
        self.set_attribute (QUEUE,            None)
        self.set_attribute (PROJECT,          None)
        self.set_attribute (CLEANUP,          None)
        self.set_attribute (_CONFIG,          None)


    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


# -----------------------------------------------------------------------------

