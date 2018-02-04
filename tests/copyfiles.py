import os
import tempfile
import saga          as rs
from ...staging_directives import complete_url


def copy_file(unit):
    uid = unit['uid']
    src_context = {'pwd'      : os.getcwd(),
                   'unit'     : unit['unit_sandbox'],
                   'pilot'    : unit['pilot_sandbox'],
                   'resource' : unit['resource_sandbox']}
    tgt_context = {'pwd'      : unit['unit_sandbox'],
                   'unit'     : unit['unit_sandbox'],
                   'pilot'    : unit['pilot_sandbox'],
                   'resource' : unit['resource_sandbox']}

    sandbox = rs.Url(unit["unit_sandbox"])
    tmp     = rs.Url(unit["unit_sandbox"])

    tmp.path = '/'
    key = str(tmp)


    if key not in self._fs_cache:
        self._fs_cache[key] = rs.filesystem.Directory(tmp, session=self._session)


    saga_dir = self._fs_cache[key]
    saga_dir.make_dir(sandbox, flags=rs.filesystem.CREATE_PARENTS)

    action = sd['action']
    flags  = sd['flags']
    did    = sd['uid']
    src    = sd['source']
    tgt    = sd['target']

    src = complete_url(src, src_context, self._log)
    tgt = complete_url(tgt, tgt_context, self._log)

    copy_flags = rs.filesystem.CREATE_PARENTS
          
    saga_dir.copy(src, tgt, flags=copy_flags)
