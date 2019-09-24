
# ------------------------------------------------------------------------------
# we *first* import radical.utils, so that the monkeypatching of the logger has
# a chance to kick in before the logging module is pulled by any other 3rd party
# module, and also to monkeypatch `os.fork()` for the `atfork` functionality.
# we also get the version string at this point.
#
import os


# ------------------------------------------------------------------------------
#
# ensure sufficient system limits on OS-X
#
if os.uname()[0] == 'Darwin':
    # on MacOS, we are running out of file descriptors soon.  The code
    # below attempts to increase the limit of open files - but any error
    # is silently ignored, so this is an best-effort, no guarantee.  We
    # leave responsibility for system limits with the user.
    #
    # FIXME: should we do this on all systems, not only Darwin (see
    #        PRTE on Summit)
    try:
        import resource
        _limits    = list(resource.getrlimit(resource.RLIMIT_NOFILE))
        _limits[0] = 512
        resource.setrlimit(resource.RLIMIT_NOFILE, _limits)
    except:
        pass


# ------------------------------------------------------------------------------
#
from .db_utils     import *
from .prof_utils   import *
from .misc         import *
from .session      import *
from .component    import *
from .slot_utils   import *


# ------------------------------------------------------------------------------

