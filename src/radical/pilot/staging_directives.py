
import os
import sys

from typing import Dict, List, Any, Union

import radical.utils as ru

from .constants import DEFAULT_ACTION, DEFAULT_FLAGS, DEFAULT_PRIORITY


# ------------------------------------------------------------------------------
#
def expand_description(descr: Dict[str, Any]) -> None:
    """Get description dictionary for simple string directive.

    Convert any simple, string based staging directive in the given task
    description into its dictionary equivalent.

    In this context, the following kinds of expansions are performed::
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
def expand_staging_directives(sds: Union[str, Dict[str, Any], List[str]]
                             ) -> List[Dict[str, Any]]:
    """Take an abbreviated or compressed staging directive and expand it."""

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
            expanded = {
                    'uid'             : ru.generate_id('sd', ru.ID_SIMPLE),
                    'source'          : src.strip(),
                    'target'          : tgt.strip(),
                    'action'          : DEFAULT_ACTION,
                    'flags'           : DEFAULT_FLAGS,
                    'priority'        : DEFAULT_PRIORITY,
                    'exception'       : None,
                    'exception_detail': None
            }

        elif isinstance(sd, dict):

            # sanity check on dict syntax
            valid_keys = ['source', 'target', 'action', 'flags', 'priority',
                          'uid', 'prof_id']
            for k in sd.keys():
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

            expanded = {
                    'uid'             : ru.generate_id('sd'),
                    'source'          : source,
                    'target'          : target,
                    'action'          : action,
                    'flags'           : flags,
                    'priority'        : priority,
                    'exception'       : None,
                    'exception_detail': None
            }

        else:
            raise Exception("Unknown directive: %s (%s)" % (sd, type(sd)))

        # we warn the user when src or tgt are using the deprecated
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
def complete_url(path   : str,
                 context: Dict[str, str],
                 log    : ru.Logger = None
                ) -> ru.Url:
    """Construct a URL.

    Some paths in data staging directives are to be interpreted relative to
    certain locations, namely relative to

    * `client://`  : the client's working directory
    * `endpoint://`: the root of the target resource's file system
    * `resource://`: the sandbox base dir on the target resource
    * `session://` : the session sandbox  on the target resource
    * `pilot://`   : the pilot   sandbox  on the target resource
    * `task://`    : the task    sandbox  on the target resource

    All location are interpreted as directories, never as files.

    For the above schemas, we interpret `schema://` the same as `schema:///`,
    ie. we treat this as a namespace, not as location qualified by a hostname.

    The `context` parameter is expected to be a dict which provides a set of
    URLs to be used to expand the path for those schemas

    Other URL schemas are left alone, any non-URL strings are interpreted as
    path in the context of `pwd`, i.e. in the context of the working directory
    of the current process, unless a `pwd` entry is provided in the `context`
    dict). `file://` schemas are left unaltered and are expected to point to
    absolute locations in the file system.

    The method returns an instance of `:py:class:`radical.utils.Url`.  Even if
    the URL is not altered, a new instance (deep copy) will be returned.

    `endpoint://` is based on the `filesystem_endpoint` attribute of the
    resource config and points to the file system accessible  via that URL.
    Note that the notion of 'root' dependends of the access protocol and the
    providing service implementation.

    Note:
        URL parsing is not really cheap, so this method should be used
        conservatively.

    """

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

    log.debug('  -> %s', purl)

    if purl.schema not in list(context.keys()):

        ret = purl
        log.debug('             = %s (%s)', ret, list(context.keys()))

    else:

        expand = True

        # we expect hostname elements to be absent for schemas we expand
        if purl.host:
            try:
                raise ValueError('URLs cannot specify `host` for expanded schemas')
            except:
                log.exception('purl host: %s' % str(purl))
                raise

        if purl.schema == 'file':
            # we leave `file://` URLs unaltered
            ret = purl
            expand = False

        elif purl.schema == 'pwd' and 'pwd' not in context:
            ret = ru.Url(os.getcwd())

        else:
            ret = ru.Url(context[purl.schema])

        if expand:
            ret.path += '/%s' % purl.path

        if expand:
            log.debug('   expand with %s', context.get(purl.schema))

    log.debug('             > %s', ret)

    return ret


# ------------------------------------------------------------------------------
