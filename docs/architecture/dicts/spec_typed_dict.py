

from mypy_extensions import TypedDict
from typing          import Dict, List, Any

import spec_attribs  as a


class MYDICT(TypedDict):

    _init = {
             a.KERNEL            : str,
             a.NAME              : str,
             a.EXECUTABLE        : str,
             a.ARGUMENTS         : List[str],
             a.ENVIRONMENT       : Dict[str, str],
             a.SANDBOX           : str,
             a.PRE_EXEC          : List[str],
             a.POST_EXEC         : List[str],
             a.RESTARTABLE       : bool,
             a.TAGS              : Dict[Any, Any],
             a.METADATA          : Any,
             a.CLEANUP           : bool,
             a.PILOT             : str,

             a.STDOUT            : str,
             a.STDERR            : str,
             a.INPUT_STAGING     : List[Any],
             a.OUTPUT_STAGING    : List[Any],

             a.CPU_PROCESSES     : int,
             a.CPU_PROCESS_TYPE  : str,
             a.CPU_THREADS       : int,
             a.CPU_THREAD_TYPE   : str,
             a.GPU_PROCESSES     : int,
             a.GPU_PROCESS_TYPE  : str,
             a.GPU_THREADS       : int,
             a.GPU_THREAD_TYPE   : str,
             a.LFS_PER_PROCESS   : int,
             a.MEM_PER_PROCESS   : int,
    }

    def __init__(self, from_dict=None):

        TypedDict.__init__(self, self._init)

        self[a.KERNEL           ] = None
        self[a.NAME             ] = None
        self[a.EXECUTABLE       ] = None
        self[a.SANDBOX          ] = None
        self[a.ARGUMENTS        ] = list()
        self[a.ENVIRONMENT      ] = dict()
        self[a.PRE_EXEC         ] = list()
        self[a.POST_EXEC        ] = list()
        self[a.STDOUT           ] = None
        self[a.STDERR           ] = None
        self[a.INPUT_STAGING    ] = list()
        self[a.OUTPUT_STAGING   ] = list()

        self[a.CPU_PROCESSES    ] = 1
        self[a.CPU_PROCESS_TYPE ] = ''
        self[a.CPU_THREADS      ] = 1
        self[a.CPU_THREAD_TYPE  ] = ''
        self[a.GPU_PROCESSES    ] = 0
        self[a.GPU_PROCESS_TYPE ] = ''
        self[a.GPU_THREADS      ] = 1
        self[a.GPU_THREAD_TYPE  ] = ''
        self[a.GPU_THREAD_TYPE  ] = ''
        self[a.LFS_PER_PROCESS  ] = 0
        self[a.MEM_PER_PROCESS  ] = 0

        self[a.RESTARTABLE      ] = False
        self[a.TAGS             ] = dict()
        self[a.METADATA         ] = None
        self[a.CLEANUP          ] = False
        self[a.PILOT            ] = ''

      # deprecated(CORES, CPU_PROCESSES)
      # deprecated(MPI,   CPU_PROCESS_TYPE)

        if from_dict:
            for k, v in from_dict.items:
                self[k] = v


