
# https://github.com/kolypto/py-good


import spec_attribs  as a
import radical.utils as ru

from good.voluptuous import *

class GOD(ru.DictMixin):

    _schema = Schema({
                      a.EXECUTABLE                   : str           ,
                      Optional(a.KERNEL            ) : str           ,
                      Optional(a.NAME              ) : str           ,
                      Optional(a.ARGUMENTS         ) : [str]         ,
                      Optional(a.ENVIRONMENT       ) : {str, str}    ,
                      Optional(a.SANDBOX           ) : str           ,
                      Optional(a.PRE_EXEC          ) : [str]         ,
                      Optional(a.POST_EXEC         ) : [str]         ,
                      Optional(a.STDOUT            ) : str           ,
                      Optional(a.STDERR            ) : str           ,
                      Optional(a.INPUT_STAGING     ) : [Any]         ,
                      Optional(a.OUTPUT_STAGING    ) : [Any]         ,

                      Optional(a.RESTARTABLE       ) : bool          ,
                      Optional(a.TAGS              ) : {Any, Any}    ,
                      Optional(a.METADATA          ) : Any           ,
                      Optional(a.CLEANUP           ) : bool          ,
                      Optional(a.PILOT             ) : str           ,

                      Optional(a.CPU_PROCESSES     ) : int           ,
                      Optional(a.CPU_PROCESS_TYPE  ) : str           ,
                      Optional(a.CPU_THREADS       ) : int           ,
                      Optional(a.CPU_THREAD_TYPE   ) : str           ,
                      Optional(a.GPU_PROCESSES     ) : int           ,
                      Optional(a.GPU_PROCESS_TYPE  ) : str           ,
                      Optional(a.GPU_THREADS       ) : int           ,
                      Optional(a.GPU_THREAD_TYPE   ) : str           ,
                      Optional(a.LFS_PER_PROCESS   ) : int           ,
                      Optional(a.MEM_PER_PROCESS   ) : int           ,
                  })


    def __init__(self):

        self._data   = dict()
    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del(self._data[key])

    def keys(self):
        return self._data.keys()

    def validate(self):

        self._schema(self._data)


