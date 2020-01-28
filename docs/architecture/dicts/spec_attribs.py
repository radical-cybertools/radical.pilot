
# ------------------------------------------------------------------------------
# Attribute description keys
NAME                   = 'name'
EXECUTABLE             = 'executable'
ARGUMENTS              = 'arguments'
ENVIRONMENT            = 'environment'
SANDBOX                = 'sandbox'

CORES                  = 'cores'  # deprecated
CPU_PROCESSES          = 'cpu_processes'
CPU_PROCESS_TYPE       = 'cpu_process_type'
CPU_THREADS            = 'cpu_threads'
CPU_THREAD_TYPE        = 'cpu_thread_type'

GPU_PROCESSES          = 'gpu_processes'
GPU_PROCESS_TYPE       = 'gpu_process_type'
GPU_THREADS            = 'gpu_threads'
GPU_THREAD_TYPE        = 'gpu_thread_type'

LFS_PER_PROCESS        = 'lfs_per_process'
MEM_PER_PROCESS        = 'mem_per_process'

INPUT_STAGING          = 'input_staging'
OUTPUT_STAGING         = 'output_staging'
PRE_EXEC               = 'pre_exec'
POST_EXEC              = 'post_exec'
KERNEL                 = 'kernel'
CLEANUP                = 'cleanup'
PILOT                  = 'pilot'
STDOUT                 = 'stdout'
STDERR                 = 'stderr'
RESTARTABLE            = 'restartable'
TAGS                   = 'tags'
METADATA               = 'metadata'

# process / thread types (for both, CPU and GPU processes/threads)
POSIX                  = 'POSIX'   # native threads / application threads
MPI                    = 'MPI'
OpenMP                 = 'OpenMP'
CUDA                   = 'CUDA'
FUNC                   = 'FUNC'

# ------------------------------------------------------------------------------
#
attribs_s       = [
                   KERNEL            ,  # str,
                   NAME              ,  # str,
                   EXECUTABLE        ,  # str,
                   PILOT             ,  # str,
                   SANDBOX           ,  # str,
                   STDOUT            ,  # str,
                   STDERR            ,  # str,
                   CPU_PROCESS_TYPE  ,  # str,
                   CPU_THREAD_TYPE   ,  # str,
                   GPU_PROCESS_TYPE  ,  # str,
                   GPU_THREAD_TYPE   ,  # str,
                  ]

attribs_b       = [
                   CLEANUP           ,  # bool,
                   RESTARTABLE       ,  # bool,
                  ]

attribs_dict_ss = [
                   ENVIRONMENT       ,  # Dict[str, str],
                  ]

attribs_dict_aa = [
                   TAGS              ,  # Dict[Any, Any],
                  ]

attribs_list_s  = [
                   ARGUMENTS         ,  # List[str],
                   PRE_EXEC          ,  # List[str],
                   POST_EXEC         ,  # List[str],
                  ]

attribs_any     = [
                   METADATA          ,  # Any,
                  ]

attribs_list_a  = [
                   INPUT_STAGING     ,  # List[Any],
                   OUTPUT_STAGING    ,  # List[Any],
                  ]

attribs_int     = [
                   CPU_PROCESSES     ,  # int,
                   CPU_THREADS       ,  # int,
                   GPU_PROCESSES     ,  # int,
                   GPU_THREADS       ,  # int,
                   LFS_PER_PROCESS   ,  # int,
                   MEM_PER_PROCESS   ,  # int,
                  ]

