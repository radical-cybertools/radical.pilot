
__copyright__ = "Copyright 2018, http://radical.rutgers.edu"
__license__ = "MIT"


from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Debug(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        '''
        This ResourceManager will digest whatever the respective configs throw at it.
        '''

        for k,v in list(cfg['resource_cfg'].items()):
            cfg[k] = v

        ResourceManager.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        cps      = self._cfg['cores_per_socket']
        gps      = self._cfg['gpus_per_socket' ]
        spn      = self._cfg['sockets_per_node']
        lpn      = self._cfg['lfs_per_node'    ]

        cpn      = (cps * spn)
        cores    = self._cfg['cores']
        nnodes   = cores / cpn + int(bool(cores % cpn))

        self.node_list = []
        for n in range(nnodes):
            self.node_list.append(['node_%d' % n, n])

        self.sockets_per_node = spn
        self.cores_per_socket = cps
        self.gpus_per_socket  = gps

        self.cores_per_node   = cps * spn
        self.gpus_per_node    = gps * spn
        self.lfs_per_node     = lpn

        self.cores_per_node   = spn * cps

        self.rm_info = {'name'               : self.name,
                        'lm_info'            : self.lm_info,
                        'node_list'          : self.node_list,
                        'agent_nodes'        : self.agent_nodes,
                        'sockets_per_node'   : self.sockets_per_node,
                        'cores_per_socket'   : self.cores_per_socket,
                        'gpus_per_socket'    : self.gpus_per_socket,
                        'lfs_per_node'       : self.lfs_per_node,
                       }

        import pprint
        pprint.pprint(self.rm_info)


# ------------------------------------------------------------------------------

