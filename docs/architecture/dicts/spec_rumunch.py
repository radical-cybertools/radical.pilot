
import radical.utils as ru
import spec_attribs  as a


class RU_MUNCH(ru.Description):

    _schema = {
               a.EXECUTABLE      : str         ,
               a.KERNEL          : str         ,
               a.NAME            : str         ,
               a.ARGUMENTS       : [str]       ,
               a.ENVIRONMENT     : {str, str}  ,
               a.SANDBOX         : str         ,
               a.PRE_EXEC        : [str]       ,
               a.POST_EXEC       : [str]       ,
               a.STDOUT          : str         ,
               a.STDERR          : str         ,
               a.INPUT_STAGING   : [None]      ,
               a.OUTPUT_STAGING  : [None]      ,

               a.RESTARTABLE     : bool        ,
               a.TAGS            : {None, None},
               a.METADATA        : None        ,
               a.CLEANUP         : bool        ,
               a.PILOT           : str         ,

               a.CPU_PROCESSES   : int         ,
               a.CPU_PROCESS_TYPE: str         ,
               a.CPU_THREADS     : int         ,
               a.CPU_THREAD_TYPE : str         ,
               a.GPU_PROCESSES   : int         ,
               a.GPU_PROCESS_TYPE: str         ,
               a.GPU_THREADS     : int         ,
               a.GPU_THREAD_TYPE : str         ,
               a.LFS_PER_PROCESS : int         ,
               a.MEM_PER_PROCESS : int         ,
    }



