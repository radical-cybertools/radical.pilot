__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.utils

import saga.attributes  as attributes
from radical.pilot.exceptions import * 

# ------------------------------------------------------------------------------
# Attribute description keys
LABEL                       = 'label'
ENABLED                     = 'enabled'
AGENT_LAUNCH_METHOD         = 'agent_launch_method'
AGENT_MONGODB_ENDPOINT      = 'agent_mongodb_endpoint'
AGENT_SCHEDULER             = 'agent_scheduler'
AGENT_SPAWNER               = 'agent_spawner'
AGENT_CONFIG                = 'agent_config'
CORES_PER_NODE              = 'cores_per_node'
GPUS_PER_NODE               = 'gpus_per_node'
DEFAULT_QUEUE               = 'default_queue'
DEFAULT_REMOTE_WORKDIR      = 'default_remote_workdir'
DESCRIPTION                 = 'description'
FILESYSTEM_ENDPOINT         = 'filesystem_endpoint'
FORWARD_TUNNEL_ENDPOINT     = 'forward_tunnel_endpoint'
JOB_MANAGER_HOP             = 'job_manager_hop'
JOB_MANAGER_ENDPOINT        = 'job_manager_endpoint'
LRMS                        = 'lrms'
MANDATORY_ARGS              = 'mandatory_args'
MPI_LAUNCH_METHOD           = 'mpi_launch_method'
NOTES                       = 'notes'
PILOT_AGENT                 = 'pilot_agent'
PRE_BOOTSTRAP_0             = 'pre_bootstrap_0'
PRE_BOOTSTRAP_1             = 'pre_bootstrap_1'
RP_VERSION                  = 'rp_version'
PYTHON_INTERPRETER          = 'python_interpreter'
SCHEMAS                     = 'schemas'
SPMD_VARIATION              = 'spmd_variation'
STAGE_CACERTS               = 'stage_cacerts'
TASK_LAUNCH_METHOD          = 'task_launch_method'
TUNNEL_BIND_DEVICE          = "tunnel_bind_device"
VALID_ROOTS                 = 'valid_roots'
VIRTENV                     = 'virtenv'
VIRTENV_MODE                = 'private'
SHARED_FILESYSTEM           = 'shared_filesystem'
HEALTH_CHECK                = 'health_check'
PYTHON_DISTRIBUTION         = 'python_dist'
VIRTENV_DISTRIBUTION        = 'virtenv_dist'
SAGA_JD_SUPPLEMENT          = 'saga_jd_supplement'
LFS_PATH_PER_NODE           = 'lfs_path_per_node'
LFS_SIZE_PER_NODE           = 'lfs_size_per_node'

# ------------------------------------------------------------------------------
#
class ResourceConfig(attributes.Attributes):
    """A ResourceConfig object describes the internal configuration parameters 
    of a compute and / or storage resource. It can be passed to a 
    :class:`radical.pilot.PilotManager` to override or extend an existing 
    configuration set. 

    **Example**::

          rc = radical.pilot.ResourceConfig(label='epsrc.archer')
          rc.job_manager_endpoint = "ssh://23.23.23.23/"
          rc.filesystem_endpoint  = "sftp://23.23.23.23"
          rc.default_queue        = "batch"
          rc.python_interpreter   = "/opt/python/2.7.6/bin/python"
          rc.pre_bootstrap_0      = "module load mpi"
          rc.valid_roots          = ["/home", "/work"]
          rc.bootstrapper         = "default_bootstrapper.sh"

          pmgr = radical.pilot.PilotManager(session=session)
          pmgr.add_resource_config(rc)

          # [...]

          pd = radical.pilot.ComputePilotDescription()
          pd.resource = "epsrc.archer"

          # [...]

          pmgr.submit_pilots(pd)

    .. parameter:: label

       [Type: `string`] [**`mandatory`**] A unique label for this configuration. 

    .. data:: enabled

       [Type: `string`] [optional] enable (default) or disable a resource entry.

    .. data:: remote_job_manager_hop

       [Type: `string`] [optional] TODO

    .. data:: remote_job_manager_endpoint

       [Type: `string`] [optional] TODO

    .. data:: remote_filesystem_endpoint

       [Type: `string`] [optional] TODO

    .. data:: local_job_manager_endpoint

       [Type: `string`] [optional] TODO

    .. data:: local_filesystem_endpoint

       [Type: `string`] [optional] TODO

    .. data:: default_queue

       [Type: `string`] [optional] TODO

    .. data:: spmd_variation

       [Type: `string`] [optional] TODO

    .. data:: python_interpreter

       [Type: `string`] [optional] TODO

    .. data:: pre_bootstrap_0

       [Type: `string`] [optional] TODO

    .. data:: pre_bootstrap_1

       [Type: `string`] [optional] TODO

    .. data:: valid_roots

       [Type: `string`] [optional] TODO

    .. data:: bootstrapper

       [Type: `string`] [optional] TODO

    .. data:: pilot_agent

       [Type: `string`] [optional] TODO

    .. data:: virtenv

       [Type: `string`] [optional] TODO

    .. data:: virtenv_mode

       [Type: `string`] [optional] TODO

    .. data:: lrms

       [Type: `string`] [optional] TODO

    .. data:: agent_scheduler

       [Type: `string`] [optional] TODO

    .. data:: shared_filesystem

       [Type: `bool`] [optional] Resource has a shared_filesystem.

    .. data:: health_check

       [Type: `bool`] [optional] Perform periodical healthcheck on agent.


    """

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def from_file(filename, entry_name=None):
      """Reads a resource configuration JSON file from the URL provided and 
         returns a list of one or more ResourceConfig objects.
      """
      rcfgs = dict()

      try:
          rcf_dict = radical.utils.read_json_str (filename)
          rcf_name = str(os.path.basename (filename))

          if  rcf_name.endswith ('.json'):
              rcf_name = rcf_name[:-5]

          if  rcf_name.startswith ('resource_'):
              rcf_name = rcf_name[9:]

          if  'aliases' in rcf_dict:
              # return empty list
              return []

          for res_name, cfg in rcf_dict.iteritems():

              # create config from resource section
              label = "%s.%s" % (rcf_name, res_name)
              rcfgs[label] = ResourceConfig(label, cfg)

      except ValueError, err:
          raise BadParameter("Couldn't parse resource configuration file '%s': %s." % (filename, str(err)))

      return rcfgs


    # --------------------------------------------------------------------------
    #
    def __init__(self, label, seeding_dict=None):
        """Optionally take a seeding dict to populate the values.
        """

        if not seeding_dict:
            seeding_dict = dict()

        # initialize attributes
        attributes.Attributes.__init__(self, seeding_dict)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register(LABEL                  , label, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ENABLED                ,  None, attributes.BOOL  , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_LAUNCH_METHOD    ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_MONGODB_ENDPOINT ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_SCHEDULER        ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_SPAWNER          ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_CONFIG           ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(CORES_PER_NODE         ,  None, attributes.INT   , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(GPUS_PER_NODE          ,  None, attributes.INT   , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_QUEUE          ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_REMOTE_WORKDIR ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DESCRIPTION            ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FILESYSTEM_ENDPOINT    ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FORWARD_TUNNEL_ENDPOINT,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(JOB_MANAGER_HOP        ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(JOB_MANAGER_ENDPOINT   ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LRMS                   ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(MANDATORY_ARGS         ,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(MPI_LAUNCH_METHOD      ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(NOTES                  ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT_AGENT            ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PRE_BOOTSTRAP_0        ,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(PRE_BOOTSTRAP_1        ,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(RP_VERSION             ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PYTHON_INTERPRETER     ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SCHEMAS                ,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(SPMD_VARIATION         ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(TASK_LAUNCH_METHOD     ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(TUNNEL_BIND_DEVICE     ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(VALID_ROOTS            ,  None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(VIRTENV                ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(VIRTENV_MODE           ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(STAGE_CACERTS          ,  None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SHARED_FILESYSTEM      ,  None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(HEALTH_CHECK           ,  None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PYTHON_DISTRIBUTION    ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(VIRTENV_DISTRIBUTION   ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SAGA_JD_SUPPLEMENT     ,  None, attributes.DICT  , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LFS_PATH_PER_NODE      ,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LFS_SIZE_PER_NODE      ,  None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)

        self['label'] = label


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


# ------------------------------------------------------------------------------
