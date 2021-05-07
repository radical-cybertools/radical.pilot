
import os
import sys

import radical.utils as ru

from .constants import DEFAULT_ACTION, DEFAULT_FLAGS, DEFAULT_PRIORITY


# ------------------------------------------------------------------------------
#
def expand_description(descr):
    """
    convert any simple, string based staging directive in the description into
    its dictionary equivalent

    In this context, the following kinds of expansions are performed:

      in:  ['input.dat']
      out: {'source' : 'client:///input.dat',
            'target' : 'task:///input.dat',
            'action' : rp.TRANSFER}

      in:  ['input.dat > staged.dat']
      out: {'source' : 'client:///input.dat',
            'target' : 'task:///staged.dat',
            'action' : rp.TRANSFER}

    This method changes the given description in place - repeated calls on the
    same description instance will have no effect.  However, we expect this
    method to be called only once during task construction.
    """

    if descr.get('input_staging')  is None: descr['input_staging']  = list()
    if descr.get('output_staging') is None: descr['output_staging'] = list()

    descr['input_staging' ] = expand_staging_directives(descr['input_staging' ])
    descr['output_staging'] = expand_staging_directives(descr['output_staging'])


# ------------------------------------------------------------------------------
#
def expand_staging_directives(sds):
    """
    Take an abbreviated or compressed staging directive and expand it.
    """


    if not sds:
        return []

    sds = ru.as_list(sds)
    ret = list()
    for sd in sds:

        if isinstance(sd, str):
            # We detected a string, convert into dict.  The interpretation
            # differs depending of redirection characters being present in the
            # string.

            if   '>>' in sd: src, tgt = sd.split('>>', 2)
            elif '>'  in sd: src, tgt = sd.split('>' , 2)
            elif '<<' in sd: tgt, src = sd.split('<<', 2)
            elif '<'  in sd: tgt, src = sd.split('<' , 2)
            else           : src, tgt = sd, os.path.basename(ru.Url(sd).path)

            # FIXME: ns = session ID
            expanded = {'source':   src.strip(),
                        'target':   tgt.strip(),
                        'action':   DEFAULT_ACTION,
                        'flags':    DEFAULT_FLAGS,
                        'priority': DEFAULT_PRIORITY,
                        'uid':      ru.generate_id('sd.%(item_counter)06d',
                                                    ru.ID_CUSTOM, ns='foo')}

        elif isinstance(sd, dict):

            # sanity check on dict syntax
            valid_keys = ['source', 'target', 'action', 'flags', 'priority',
                          'uid', 'prof_id']
            for k in sd:
                if k not in valid_keys:
                    raise ValueError('"%s" is invalid on staging directive' % k)

            source   = sd.get('source')
            target   = sd.get('target',   os.path.basename(ru.Url(source).path))
            action   = sd.get('action',   DEFAULT_ACTION)
            flags    = sd.get('flags',    DEFAULT_FLAGS)
            priority = sd.get('priority', DEFAULT_PRIORITY)

            if not source:
                raise Exception("Staging directive dict has no source member!")

            # RCT flags should always be rendered as OR'ed integers - but old
            # versions of the RP API rendered them as list of strings.  We
            # convert to the integer version for backward compatibility - but we
            # complain loudly if we find actual strings.
            if isinstance(flags, list):
                int_flags = 0
                for flag in flags:
                    if isinstance(flags, str):
                        raise ValueError('"%s" is no valid RP constant' % flag)
                    int_flags |= flag
                flags = int_flags

            elif isinstance(flags, str):
                raise ValueError('use RP constants for staging flags!')

            expanded = {'uid':      ru.generate_id('sd'),
                        'source':   source,
                        'target':   target,
                        'action':   action,
                        'flags':    flags,
                        'priority': priority}

        else:
            raise Exception("Unknown directive: %s (%s)" % (sd, type(sd)))

        # we warn the user when  src or tgt are using the deprecated
        # `staging://` schema
        if str(expanded['source']).startswith('staging://'):
            sys.stderr.write('staging:// schema is deprecated - use pilot://\n')
            expanded['source'] = str(expanded['source']).replace('staging://',
                                                                 'pilot://')

        if str(expanded['target']).startswith('staging://'):
            sys.stderr.write('staging:// schema is deprecated - use pilot://\n')
            expanded['target'] = str(expanded['target']).replace('staging://',
                                                                 'pilot://')

        ret.append(expanded)

    return ret


# ------------------------------------------------------------------------------
#
def complete_url(path, context, log=None):
    '''
    Some paths in data staging directives are to be interpreted relative to
    certain locations, namely relative to

        * `client://`  : the client's working directory
        * `resource://`: the RP    sandbox on the target resource
        * `pilot://`   : the pilot sandbox on the target resource
        * `task://`    : the task  sandbox on the target resource

    For the above schemas, we interpret `schema://` the same as `schema:///`,
    ie. we treat this as a namespace, not as location qualified by a hostname.

    The `context` parameter is expected to be a dict which provides a set of
    URLs to be used to expand the path.

    Other URL schemas are left alone, any other strings are interpreted as
    path in the context of `pwd`.

    The method returns an instance of ru.Url.  Note that URL parsing is not
    really cheap, so this method should be used conservatively.
    '''

    # FIXME: consider evaluation of env vars

    purl     = ru.Url(path)
    str_path = str(path)

    # we always want a schema, and fall back to file:// or pwd://, depending if
    # the path is absolute or relative.  Note that pwd:// is interpreted in the
    # context of the staging component, which may live on the client or target
    # resource -- we make no attempt at expanding the path at this point.
    # We further assume that the user knows what she is doing when using
    # absolute paths, and make no attempts to verify those either.
    if not purl.schema:
        if str_path.startswith('/'):
            purl.schema = 'file'
        else:
            purl.schema = 'pwd'

    schema = purl.schema

    if schema == 'client':
        # 'client' is 'pwd' in client context.
        # FIXME: We don't check context though.
        schema = 'pwd'

    if schema in list(context.keys()):

        # we interpret any hostname as part of the path element
        if   purl.host and purl.path: ppath = '%s/%s' % (purl.host, purl.path)
        elif purl.host              : ppath =    '%s' %            (purl.host)
        elif purl.path              : ppath =    '%s' %            (purl.path)
        else                        : ppath =     '.'

        if schema not in context:
            raise ValueError('cannot expand schema (%s) for staging' % schema)

        log.debug('   expand with %s', context[schema])
        ret = ru.Url(context[schema])

        ret.path += '/%s' % ppath
        purl      = ret

    # if not schema is set, assume file:// on localhost
    if not purl.schema:
        purl.schema = 'file'

    return purl


# ------------------------------------------------------------------------------

