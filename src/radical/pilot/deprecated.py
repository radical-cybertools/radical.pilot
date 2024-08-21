
import sys


# ------------------------------------------------------------------------------
#
# make sure deprecation warning is shown only once per type
#
_seen = list()


def _warn(old_type, new_type):
    if old_type not in _seen:
        _seen.append(old_type)
        sys.stderr.write('%s is deprecated - use %s\n' % (old_type, new_type))
        sys.stderr.flush()


# ------------------------------------------------------------------------------

