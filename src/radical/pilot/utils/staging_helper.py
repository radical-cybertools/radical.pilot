
import os
import shutil
import requests

import radical.utils as ru

from ..constants import COPY, LINK, MOVE, TRANSFER, DOWNLOAD
from ..constants import TARBALL  # , CREATE_PARENTS, RECURSIVE


# ------------------------------------------------------------------------------
#
class StagingHelper(object):

    def __init__(self, log):

        self._log  = log

        try   : self._backend = StagingHelper_SAGA (self._log)
        except: self._backend = StagingHelper_Local(self._log)

        log.debug('using staging backend %s' % self._backend.__class__.__name__)


    def mkdir(self, tgt, flags=None):
        self._log.debug('mkdir %s', tgt)
        self._backend.mkdir(tgt, flags)

    def rmdir(self, tgt, flags=None):
        self._log.debug('rmdir %s', tgt)
        self._backend.rmdir(tgt, flags)

    def copy(self, src, tgt, flags=None):
        self._log.debug('copy  %s %s', src, tgt)
        self._backend.copy(src, tgt, flags)

    def move(self, src, tgt, flags=None):
        self._log.debug('move  %s %s', src, tgt)
        self._backend.move(src, tgt, flags)

    def link(self, src, tgt, flags=None):
        self._log.debug('link  %s %s', src, tgt)
        self._backend.link(src, tgt, flags)

    def download(self, src, tgt, flags=None):
        self._log.debug('download %s %s', src, tgt)
        self._backend.download(src, tgt, flags)

    def delete(self, tgt, flags=None):
        self._log.debug('rm    %s', tgt)
        self._backend.delete(tgt, flags)

    def sh_callout(self, url, cmd):
        self._log.debug('shcmd %s %s', url, cmd)
        return self._backend.sh_callout(url, cmd)

    def handle_staging_directive(self, sd):

        action  = sd['action']
        src     = sd['source']
        tgt     = sd['target']
        flags   = sd.get('flags', 0)

        assert action in [COPY, LINK, MOVE, TRANSFER, DOWNLOAD]

        self._log.info('%-10s %s', action, src)
        self._log.info('%-10s %s', '', tgt)

        if action in [COPY, TRANSFER]:
            self.copy(src, tgt, flags)

        elif action == LINK:
            self.link(src, tgt, flags)

        elif action == MOVE:
            self.move(src, tgt, flags)

        elif action in [DOWNLOAD]:
            self.download(src, tgt, flags)


# ------------------------------------------------------------------------------
#
class StagingHelper_Local(object):

    def __init__(self, log):
        self._log = log

    def mkdir(self, tgt, flags):
        self._log.debug('mkdir %s', tgt)
        tgt = ru.Url(tgt).path
        ru.rec_makedir(tgt)

    def rmdir(self, tgt, flags):
        tgt = ru.Url(tgt).path
        os.rmdir(tgt)

    def copy(self, src, tgt, flags):
        src = ru.Url(src).path
        tgt = ru.Url(tgt).path
        self.mkdir(os.path.dirname(tgt), flags)
        ru.sh_callout('cp -r %s %s' % (src, tgt))

    def move(self, src, tgt, flags):
        src = ru.Url(src).path
        tgt = ru.Url(tgt).path
        self.mkdir(os.path.dirname(tgt), flags)
        shutil.move(src, tgt)

    def link(self, src, tgt, flags):
        src = ru.Url(src).path
        tgt = ru.Url(tgt).path
        self.mkdir(os.path.dirname(tgt), flags)
        os.link(src, tgt)

    def download(self, src, tgt, flags):
        tgt = ru.Url(tgt).path
        self.mkdir(os.path.dirname(tgt), flags)
        r = requests.get(src, stream=True)
        with open(tgt, 'wb') as fout:
            for chunk in r.iter_content():
                fout.write(chunk)

    def delete(self, tgt, flags):
        tgt = ru.Url(tgt).path
        try   : os.unlink(tgt)
        except: pass

    def sh_callout(self, url, cmd):
        return ru.sh_callout(cmd, shell=True)


# ------------------------------------------------------------------------------
#
class StagingHelper_SAGA(object):

    try:
        import radical.saga.filesystem      as _rsfs
        import radical.saga.utils.misc      as _rsum
        import radical.saga.utils.pty_shell as _rsup
        _has_saga = True
    except:
        _has_saga = False

    def __init__(self, log):
        self._log = log
        if not self._has_saga:
            raise Exception('SAGA-Python not available')

    def mkdir(self, tgt, flags):
        assert self._has_saga


    def rmdir(self, tgt, flags):
        assert self._has_saga


    def copy(self, src, tgt, flags):

        src = ru.Url(src)
        tgt = ru.Url(tgt)

      # # FIXME: why??
      # flags = 0

        src = ru.Url(src)
        tgt = ru.Url(tgt)

        assert self._has_saga

        tmp      = ru.Url(tgt)
        tmp.path = '/'

        fs     = self._rsfs.Directory(str(tmp))
        flags |= self._rsfs.CREATE_PARENTS

        if os.path.isdir(src.path) or src.path.endswith('/'):
            flags |= self._rsfs.RECURSIVE

      # self._log.debug("copy %s 1 -> %s [%s]" % (src, tgt, flags))
        fs.copy(src, tgt, flags=flags)

    def move(self, src, tgt, flags):
        assert self._has_saga


    def link(self, src, tgt, flags):
        assert self._has_saga

    def download(self, src, tgt, flags):
        assert self._has_saga

        self.copy(src, tgt, flags)


    def delete(self, tgt, flags):
        assert self._has_saga

    def sh_callout(self, url, cmd):
        assert self._has_saga

        js_url = ru.Url(url)
        elems  = js_url.schema.split('+')

        if   'ssh'    in elems: js_url.schema = 'ssh'
        elif 'gsissh' in elems: js_url.schema = 'gsissh'
        elif 'fork'   in elems: js_url.schema = 'fork'
        elif len(elems) == 1  : js_url.schema = 'fork'
        else: raise Exception("invalid schema: %s" % js_url.schema)

        if js_url.schema == 'fork':
            js_url.host = 'localhost'

        self._log.debug("_rsup.PTYShell('%s')", js_url)
        shell = self._rsup.PTYShell(js_url)

        ret, out, err = shell.run_sync(cmd)

        return out, err, ret


# ------------------------------------------------------------------------------

