
# https://pydantic-docs.helpmanual.io/usage/dataclasses/


from mypy_extensions import TypedDict
from typing          import Dict, List, Any, Optional

import pydantic

import spec_attribs  as a

import radical.utils as ru


class PYD(pydantic.BaseModel):

    kernel            : Optional[str]                = None
    name              : Optional[str]                = None
    executable        : Optional[str]                = None
    arguments         : Optional[List[str]]          = None
    environment       : Optional[Dict[str, str]]     = list()
    sandbox           : Optional[str]                = dict()
    pre_exec          : Optional[List[str]]          = list()
    post_exec         : Optional[List[str]]          = list()
    stdout            : Optional[str]                = None
    stderr            : Optional[str]                = None
    input_staging     : Optional[List[Any]]          = list()
    output_staging    : Optional[List[Any]]          = list()

    restartable       : Optional[bool]               = False
    tags              : Optional[Dict[Any, Any]]     = dict()
    metadata          : Optional[Any]                = None
    cleanup           : Optional[bool]               = False
    pilot             : Optional[str]                = ''

    cpu_processes     : Optional[int]                = 1
    cpu_process_type  : Optional[str]                = ''
    cpu_threads       : Optional[int]                = 1
    cpu_thread_type   : Optional[str]                = ''
    gpu_processes     : Optional[int]                = 0
    gpu_process_type  : Optional[str]                = ''
    gpu_threads       : Optional[int]                = 1
    gpu_thread_type   : Optional[str]                = ''
    lfs_per_process   : Optional[int]                = 0
    mem_per_process   : Optional[int]                = 0


    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)

    def validate(self):
        pass


