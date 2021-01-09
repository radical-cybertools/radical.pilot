
import os

import radical.utils as ru

from .constants import DEFAULT_ACTION, DEFAULT_FLAGS, DEFAULT_PRIORITY

_IN  = 0
_OUT = 1


# ------------------------------------------------------------------------------
#
def expand_description(descr):
    '''
    convert any simple, string based staging directive in the description into
    its dictionary equivalent

    In this context, the following kinds of expansions are performed:

      in:  ['input.dat']
      out: {'source' : 'sandbox://client/input.dat',
            'target' : 'sandbox://task/input.dat',
            'action' : rp.TRANSFER}

      in:  ['input.dat > staged.dat']
      out: {'source' : 'sandbox://client/input.dat',
            'target' : 'sandbox://task/staged.dat',
            'action' : rp.TRANSFER}

    This method changes the given description in place.
    '''

    if descr.get('input_staging')  is None: descr['input_staging']  = list()
    if descr.get('output_staging') is None: descr['output_staging'] = list()

    descr['input_staging' ] = expand_sds(descr['input_staging' ], 'task')
    descr['output_staging'] = expand_sds(descr['output_staging'], 'client')

    return descr


# ------------------------------------------------------------------------------
#
def expand_sds(sds, sandbox):
    '''
    Take an abbreviated or compressed staging directive, expand it, and expand
    sandboxes
    '''

    if not sds:
        return []

    ret = list()
    sds = ru.as_list(sds)

    for sd in sds:

        if isinstance(sd, str):

            # We detected a string, convert into dict.  The interpretation
            # differs depending of redirection character being present in the
            # string.

            if   '>'  in sd: src, tgt = sd.split('>' , 2)
            elif '<'  in sd: tgt, src = sd.split('<' , 2)
            else           : src, tgt = sd, os.path.basename(ru.Url(sd).path)

            # FIXME: ns = session ID
            expanded = {
                    'source':   src.strip(),
                    'target':   tgt.strip(),
                    'action':   DEFAULT_ACTION,
                    'flags':    DEFAULT_FLAGS,
                    'priority': DEFAULT_PRIORITY,
            }

        elif isinstance(sd, dict):

            # sanity check on dict syntax
            valid_keys = ['source', 'target', 'action', 'flags', 'priority',
                          'uid', 'prof_id']
            for k in sd:
                if k not in valid_keys:
                    raise ValueError('"%s" is invalid on staging directive' % k)

            src = sd.get('source')
            tgt = sd.get('target', os.path.basename(ru.Url(src).path))

            action   = sd.get('action',   DEFAULT_ACTION)
            flags    = sd.get('flags',    DEFAULT_FLAGS)
            priority = sd.get('priority', DEFAULT_PRIORITY)

            assert(src)

            # RCT flags should always be rendered as OR'ed integers - but old
            # versions of the RP API rendered them as list of strings.  We
            # convert to the integer version for backward compatibility - but we
            # complain loudly if we find actual strings.
            if isinstance(flags, str):
                raise ValueError('use RP constants for staging flags!')

            int_flags = 0
            for flag in ru.as_list(flags):
                if isinstance(flags, str):
                    raise ValueError('"%s" is no valid RP constant' % flag)
                int_flags |= flag
            flags = int_flags

            # FIXME: ID ns = session ID
            expanded = {'source':   src,
                        'target':   tgt,
                        'action':   action,
                        'flags':    flags,
                        'priority': priority}

        else:
            raise TypeError('cannot handle SD type %s' % type(sd))


        expanded['uid'] = ru.generate_id('sd')
        ret.append(expanded)

        # FIXME: expand sandboxes
        # FIXME: move to session

    return ret


# ------------------------------------------------------------------------------
#
def complete_url(path, contexts):
    '''
    Some paths in data staging directives are to be interpreted relative to
    certain locations, namely relative to

        * `sandbox://client/`  : the client's working directory
        * `sandbox://resource/`: the RP    sandbox on the target resource
        * `sandbox://pilot/`   : the pilot sandbox on the target resource
        * `sandbox://task/`    : the task  sandbox on the target resource
        * `sandbox://<uid>/`   : the task  sandbox for task with given uid

    The `context` parameter is expected to be a dict which provides a set of
    URLs to be used to expand the path:

        {
          'client': 'file://localhost/foo/bar',
          'pilot' : 'sftp://host.net/tmp/session.0000/pilot.000',
          'pwd'   : '/tmp/session.0000',
          ...
        }

    URL schemas other than `sandbox://` are left alone, values without URL
    schema are interpreted as path (either absolute or relative with respect to
    `pwd`)

    The method returns an instance of ru.Url.  If an expansion is not possible,
    the return value is None.

    Note that URL parsing is not really cheap, so this method should be used
    conservatively.
    '''

    assert(ru.is_string(path) and path)

    # We assume that the user knows what she is doing when using absolute paths,
    # and make no attempts to verify those.
    if str(path)[0] == '/':
        return ru.Url('file://localhost/' + path)

    # we always want a schema, otherwise interpret as path relative to `pwd`
    if '://' not in path:
        # no schema: path relative to `pwd`
        for context in contexts:
            if 'pwd' not in context:
                return ru.Url(context['pwd'] + '/' + path)

    # only expand sandbox schemas
    if not path.startswith('sandbox://'):
        return ru.Url(path)

    _, _, host, rest = path.split('/', 3)
    assert(host)
    assert(rest)

    # backward compatibility
    if host == 'unit': host = 'task'

    # need to do sandbox expansion
    for context in contexts:
        if host in context:
            return ru.Url(context[host] + '/' + rest)

    # cannot expand: this likely refers to an unknown UID
    raise ValueError('cannot expand %s', path)


# ------------------------------------------------------------------------------

