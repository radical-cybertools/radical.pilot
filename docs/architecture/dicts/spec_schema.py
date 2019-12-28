
# https://github.com/keleshev/schema

from schema import Schema, And, Use, Optional, Or

import spec_attribs  as a


schema = Schema([{'name': And(str, len),
                  'age':  And(Use(int), lambda n: 18 <= n <= 99),
                  Optional('gender'): And(str, Use(str.lower),
                                          lambda s: s in ('squid', 'kid'))}])


class SCH(dict):

    _any    = Or(object, int, float, None)
    _schema = Schema({
                      a.EXECUTABLE         : str,
             Optional(a.KERNEL          )  : str,
             Optional(a.NAME            )  : str,
             Optional(a.ARGUMENTS       )  : [str],
             Optional(a.ENVIRONMENT     )  : Or({}, {str: str}),
             Optional(a.SANDBOX         )  : str,
             Optional(a.PRE_EXEC        )  : [str],
             Optional(a.POST_EXEC       )  : [str],
             Optional(a.RESTARTABLE     )  : bool,
             Optional(a.TAGS            )  : {_any: _any},
             Optional(a.METADATA        )  : object,
             Optional(a.CLEANUP         )  : bool,
             Optional(a.PILOT           )  : str,

             Optional(a.STDOUT          )  : str,
             Optional(a.STDERR          )  : str,
             Optional(a.INPUT_STAGING   )  : [_any],
             Optional(a.OUTPUT_STAGING  )  : [_any],

             Optional(a.CPU_PROCESSES   )  : int,
             Optional(a.CPU_PROCESS_TYPE)  : str,
             Optional(a.CPU_THREADS     )  : int,
             Optional(a.CPU_THREAD_TYPE )  : str,
             Optional(a.GPU_PROCESSES   )  : int,
             Optional(a.GPU_PROCESS_TYPE)  : str,
             Optional(a.GPU_THREADS     )  : int,
             Optional(a.GPU_THREAD_TYPE )  : str,
             Optional(a.LFS_PER_PROCESS )  : int,
             Optional(a.MEM_PER_PROCESS )  : int,
    })

    def __init__(self, from_dict=None):

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

    def validate(self):
        assert(self._schema.validate(self))


