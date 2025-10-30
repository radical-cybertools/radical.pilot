
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from .task_description import TaskDescription
from .utils.misc       import FastTypedDict

# ------------------------------------------------------------------------------
# Attribute description keys
#

UID               = 'uid'
RESOURCE          = 'resource'
ACCESS_SCHEMA     = 'access_schema'
QUEUE             = 'queue'
JOB_NAME          = 'job_name'
PROJECT           = 'project'
SANDBOX           = 'sandbox'
RUNTIME           = 'runtime'
APP_COMM          = 'app_comm'         # FIXME: always create an app pubsub?
CLEANUP           = 'cleanup'
EXIT_ON_ERROR     = 'exit_on_error'
SERVICES          = 'services'
ENABLE_EP         = 'enable_ep'
RECONFIG_SRC      = 'reconfig_src'

NODES             = 'nodes'
BACKUP_NODES      = 'backup_nodes'
CORES             = 'cores'
GPUS              = 'gpus'
MEMORY            = 'memory'

INPUT_STAGING     = 'input_staging'
OUTPUT_STAGING    = 'output_staging'

PREPARE_ENV       = 'prepare_env'


# ------------------------------------------------------------------------------
#
class PilotDescription(FastTypedDict):
    """Specify a requested Pilot.

    A PilotDescription object describes the requirements and properties
    of a :class:`radical.pilot.Pilot` and is passed as a parameter to
    :func:`radical.pilot.PilotManager.submit_pilots` to instantiate and run
    a new pilot.

    Note:
        A PilotDescription **MUST** define at least :attr:`resource`,
        :attr:`cores` or :attr:`nodes`, and :attr:`runtime`.

        If :attr:`backup_nodes` is set, then :attr:`nodes` must also be set.


    Example::
        pm = radical.pilot.PilotManager(session=s)
        pd = radical.pilot.PilotDescription()
        pd.resource = "local.localhost"
        pd.nodes    = 2
        pd.runtime  = 5 # minutes

        pilot = pm.submit_pilots(pd)

    Attributes:
        uid (str, optional): A unique ID for the pilot. A unique ID will be
            assigned by RP if the field is not set.
        job_name (str, optional):  The name of the job / pilot as provided to
            the batch system. If not set then :attr:`uid` will be used instead.
        resource (str): The key of a
            :ref:`platform description </tutorials/configuration.ipynb#Platform-description>`
            entry. If the key exists, the machine-specific
            configuration is loaded from the config file once the
            `PilotDescription` is passed to
            :meth:`radical.pilot.PilotManager.submit_pilots`. If the key doesn't
            exist, an exception :class:`ValueError` is raised.  access_schema
            (str, optional): The key of an access mechanism to use.  The valid
            access mechanism is defined in the resource configuration.  See
            :doc:`/tutorials/configuration`. The first ``schema`` defined in the
            resource configuration is used by default, if no *access_schema* is
            specified.
        runtime (int, optional): The maximum run time (wall-clock time) in
            **minutes** of the pilot. Default 10.
        sandbox (str, optional): The working ("sandbox") directory of the pilot
            agent.  This parameter is optional and if not set, it defaults to
            *radical.pilot.sandbox* in your home or login directory.
            Default None.

            Warning:
                If you define a pilot on an HPC cluster and you want to set
                `sandbox` manually, make sure that it points to a directory
                on a shared filesystem that can be reached from all
                compute nodes.

        nodes (int, optional): The number of nodes the pilot should allocate on
            the target resource. This parameter could be set instead of `cores`
            and `gpus` (and `memory`). Default 1.

            Note:
                If `nodes` is specified, `gpus` and `cores` must not be
                specified.

        backup_nodes (int, optional): The number of backup nodes the pilot will
            allocate.  Those nodes will be swapped-in in case of node failures.

        cores (int, optional): The number of cores the pilot should allocate on
            the target resource. This parameter could be set instead of `nodes`.

            Note:
                For local pilots, you can set a number larger than the physical
                machine limit (corresponding resource configuration should have
                the attribute `"fake_resources"`).

            Note:
                If `cores` is specified, `nodes` must not be specified.

        gpus (int, optional): The number of gpus the pilot should allocate on
            the target resource.

            Note:
                If `gpus` is specified, `nodes` must not be specified.

        memory (int, optional): The total amount of physical memory the
            pilot (and related to it job) requires.
        queue (str, optional): The name of the job queue the pilot should get
            submitted to. If *queue* is set in the resource configuration
            (:attr:`resource`), defining `queue` will override it explicitly.
        project (str, optional): The name of the project / allocation to
            charge for used CPU time. If *project* is set in the resource
            configuration (:attr:`resource`), defining `project` will override
            it explicitly.
        app_comm (list[str], optional): The list of names is interpreted as
            communication channels to start within the pilot agent, for the
            purpose of application communication, i.e., that tasks running on
            that pilot are able to use those channels to communicate amongst
            each other.

            The names are expected to end in *_queue* or *_pubsub*, indicating
            the type of channel to create.  Once created, tasks will find
            environment variables of the name ``RP_%s_IN`` and ``RP_%s_OUT``,
            where ``%s`` is replaced with the given channel name (uppercased),
            and ``IN/OUT`` indicate the respective endpoint addresses for the
            created channels.
        input_staging (list, optional): The list of files to be staged into the
            pilot sandbox.
        output_staging (list, optional): The list of files to be staged from the
            pilot sandbox.
        cleanup (bool, optional): If cleanup is set to True, the pilot
            will delete its entire sandbox upon termination. This includes
            individual Task sandboxes and all generated output data. Only log
            files will remain in the sandbox directory. Default False.
        exit_on_error (bool, optional): Flag to trigger app termination in case
            of the pilot failure. Default True.
        services (list[TaskDescription], optional): A list of
            commands which get started on a separate service compute node right
            after bootstrapping, and before any RP task is launched.  That
            service compute node will not be used for any other tasks.
        enable_ep (bool, optional): enable a ZMQ submission endpoint on the
            pilot. Default `False`.
        prepare_env (dict, optional): A dictionary of `{env_name: env_spec}` as
            documented for the pilot's `prepare_env(env_name, env_spec)` method.
            The given specifications will be enacted during pilot startup and
            can be used for service tasks.
        reconfig_src (string, optional): name of a data file to be used by the
            agent's `reconfig` scheduler.

    """

    _schema = {
        UID             : str        ,
        RESOURCE        : str        ,
        ACCESS_SCHEMA   : str        ,
        RUNTIME         : int        ,
        APP_COMM        : [str]      ,
        SANDBOX         : str        ,
        CORES           : int        ,
        GPUS            : int        ,
        NODES           : int        ,
        BACKUP_NODES    : int        ,
        MEMORY          : int        ,
        QUEUE           : str        ,
        JOB_NAME        : str        ,
        PROJECT         : str        ,
        CLEANUP         : bool       ,
        EXIT_ON_ERROR   : bool       ,
        INPUT_STAGING   : [str]      ,
        OUTPUT_STAGING  : [str]      ,
        PREPARE_ENV     : {str: None},
        SERVICES        : [TaskDescription],
        ENABLE_EP       : bool       ,
        RECONFIG_SRC    : str        ,
    }

    _defaults = {
        UID             : None       ,
        RESOURCE        : None       ,
        ACCESS_SCHEMA   : None       ,
        RUNTIME         : 10         ,
        APP_COMM        : []         ,
        SANDBOX         : None       ,
        CORES           : 0          ,
        GPUS            : 0          ,
        NODES           : 0          ,
        BACKUP_NODES    : 0          ,
        MEMORY          : 0          ,
        QUEUE           : None       ,
        JOB_NAME        : None       ,
        PROJECT         : None       ,
        CLEANUP         : False      ,
        EXIT_ON_ERROR   : True       ,
        INPUT_STAGING   : []         ,
        OUTPUT_STAGING  : []         ,
        PREPARE_ENV     : {}         ,
        SERVICES        : []         ,
        ENABLE_EP       : False      ,
        RECONFIG_SRC    : None       ,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        if not self.get('resource'):
            raise ValueError("Pilot description needs 'resource'")

        if self.get('backup_nodes') and not self.get('nodes'):
            raise ValueError("'nodes' must be set if 'backup_nodes' is set")

        if not self.get('nodes'):
            if not self.get('cores'):
                raise ValueError("Pilot description needs 'nodes' or 'cores'")

        else:
            if self.get('cores'):
                raise ValueError("Pilot description needs 'cores' *or* 'nodes'")

            if self.get('gpus'):
                raise ValueError("Pilot description needs 'cores' *or* 'nodes'")


# ------------------------------------------------------------------------------

