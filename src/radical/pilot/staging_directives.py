
import os

import radical.utils as ru

from .constants import *

# The Staging Directives are specified using a dict in the following form:
#   staging_directive = {
#       'source':   None, # ru.Url() or string
#       'target':   None, # ru.Url() or string
#       'action':   None, # See 'Action operators' below
#       'flags':    None, # See 'Flags' below
#       'priority': 0     # Control ordering of actions
#   }

#
# Action operators
#
# ------------------------------------------------------------------------------
#
def expand_description(descr):
    """
    convert any simple, string based staging directive in the description into 
    its dictionary equivalent

    In this context, the following kinds of expansions are performed:

      in:  ['input.dat'] 
      out: {'source' : 'client:///input.dat', 
            'target' : 'unit:///input.dat',
            'action' : rp.TRANSFER}

      in:  ['input.dat > staged.dat']
      out: {'source' : 'client:///input.dat', 
            'target' : 'unit:///staged.dat',
            'action' : rp.TRANSFER}

    This method changes the given description in place - repeated calls on the
    same description instance will have no effect.  However, we expect this
    method to be called only once during unit construction.
    """

    descr['input_staging']  = expand_staging_directives(descr.get('input_staging' ))
    descr['output_staging'] = expand_staging_directives(descr.get('output_staging'))


# ------------------------------------------------------------------------------
#
def complete_description(descr, unit):
    '''
    For all staging directives in the description, expand the given URLs to
    point to the right sandboxes, where required.  We do not alter the
    description itself, so we can perform the same completion again later on,
    possibly after the sandbox locations changed due to rescheduling etc.
    Instead, we return two lists of staging directives, for input and output 
    staging, respectively.

    Completion is performed on URLs of the following types:

        resource:///path
        pilot:///path
        unit:///path
        client:///path

    The userauth and hostname elements of the URL must be empty, the schema must
    match one of the ones given above.  In those cases, the `path` specification
    is interpreted as *relative* path (ie. withtou the leasing slash), in
    relation to the known resource, pilot, and unit sandboxes, or in relation to
    the client workdir.  All other URLs are interpreted verbatim, and the `path`
    element is interpreted as absolute path in the respective file system.
    '''

    input_sds  = list()
    output_sds = list()

    for sd in descr.get('input_staging'):

        source = _complete_url(sd['source'], unit)
        target = _complete_url(sd['target'], unit)

        input_sds.append({'source' : source, 
                          'target' : target, 
                          'action' : sd['action']}

    for sd in descr.get('output_staging'):

        source = _complete_url(sd['source'], unit)
        target = _complete_url(sd['target'], unit)

        output_sds.append({'source' : source, 
                           'target' : target, 
                           'action' : sd['action']}

    return [input_sds, output_sds]


# ------------------------------------------------------------------------------
def _complete_url(path, unit):
    '''
    Some paths in data staging directives are to be interpreted as relative to
    `complete_description()` above.  
    '''

    # FIXME: consider evaluation of env vars
    # FIXME: maybe support abs path on completed URLs, too



    # nothing done for URLs, those are always absolute
    if isinstance(path, ru.Url):
        return str(path)

    # if `://` is part of `path`, its likely a URL anyway, and we
    # convert/reparse it
    if '://' in path:
        path = ru.Url(path).path

    if path.startswith('/'): is_abs = True
    else                   : is_abs = False

    url = ru.Url(path)

    if url.schema in [None, '', 'file'     ] and \
       url.host   in [None, '', 'localhost'] :
        url.schema = 'file'
        url.host   = 'localhost'
        if is_abs: url.path = path
        else     : url.path = '%s/%s' % (os.getcwd(), url.path)
        # FIXME: the above uses pwd on the client side, but the staging
        #        directive may get interpreted at the agent side

    return str(url)


# ------------------------------------------------------------------------------
#
def expand_staging_directives(staging_directives):
    """
    Take an abbreviated or compressed staging directive and expand it.
    """

    log = ru.get_logger('radical.pilot.utils')

    if not staging_directives:
        return []

    if not isinstance(staging_directives, list):
        staging_directives = [staging_directives]

    ret = []

    # We loop over the list of staging directives
    for sd in staging_directives:

        if isinstance(sd, basestring):

            # We detected a string, convert into dict.  The interpretation
            # differs depending of redirection characters being present in the
            # string.

            append = False
            if '>>'  in sd:
                src, tgt = sd.split('>>', 2)
                append = True
            elif '>' in sd :
                src, tgt = sd.split('>',  2)
                append  = False
            elif '<<' in sd:
                tgt, src = sd.split('<<', 2)
                append = True
            elif '<'  in sd:
                tgt, src = sd.split('<',  2)
                append = False
            else:
                src, tgt = sd, os.path.basename(sd)
                append = False

            if append:
                log.warn("append mode on staging not supported (ignored)")

            new_sd = {'uid':      ru.generate_id('sd'),
                      'source':   src.strip(),
                      'target':   tgt.strip(),
                      'action':   DEFAULT_ACTION,
                      'flags':    DEFAULT_FLAGS,
                      'priority': DEFAULT_PRIORITY
            }
            log.debug("Converting string '%s' into dict '%s'" % (sd, new_sd))
            ret.append(new_sd)

        elif isinstance(sd, dict):

            # sanity check on dict syntax
            valid_keys = ['source', 'target', 'action', 'flags', 'priority']
            for k in sd:
                if k not in valid_keys:
                    raise ValueError('invalif entry "%s" on staging directive' % k)

            # We detected a dict, will have to distinguish between single and multiple entries
            if 'action' in sd:
                action = sd['action']
            else:
                action = DEFAULT_ACTION

            if 'flags' in sd:
                flags = sd['flags']
            else:
                flags = DEFAULT_FLAGS

            if 'priority' in sd:
                priority = sd['priority']
            else:
                priority = DEFAULT_PRIORITY

            if not 'source' in sd:
                raise Exception("Staging directive dict has no source member!")
            source = sd['source']

            if 'target' in sd:
                target = sd['target']
            else:
                # Set target to None, as inferring it depends on the type of source
                target = None

            if isinstance(source, basestring) or isinstance(source, ru.Url):

                if target:
                    # Detect asymmetry in source and target length
                    if isinstance(target, list):
                        raise Exception("Source is singular but target is a list")
                else:
                    # We had no target specified, assume the basename of source
                    if isinstance(source, basestring):
                        target = os.path.basename(source)
                    elif isinstance(source, ru.Url):
                        target = os.path.basename(source.path)
                    else:
                        raise Exception("Source %s is neither a string nor a Url (%s)!" %
                                        (source, type(source)))

                # This is a regular entry, complete and append it
                new_sd = {'uid':      ru.generate_id('sd'),
                          'source':   source,
                          'target':   target,
                          'action':   action,
                          'flags':    flags,
                          'priority': priority,
                }
                ret.append(new_sd)
                log.debug("Completing entry '%s'" % new_sd)

            elif isinstance(source, list):
                # We detected a list of sources, we need to expand it

                # We will break up the list entries in source into an equal length list of dicts
                new_sds = []

                if target:
                    # Target is also specified, make sure it is a list of equal length

                    if not isinstance(target, list):
                        raise Exception("Both source and target are specified, but target is not a list")

                    if len(source) != len(target):
                        raise Exception("Source (%d) and target (%d) are lists of different length" % (len(source), len(target)))

                    # Now that we have established that the list are of equal size we can combine them
                    for src_entry, tgt_entry in zip(source, target):

                        new_sd = {'uid':      ru.generate_id('sd'),
                                  'source':   src_entry,
                                  'target':   tgt_entry,
                                  'action':   action,
                                  'flags':    flags,
                                  'priority': priority
                        }
                        new_sds.append(new_sd)
                else:
                    # Target is not specified, use the source for the target too.

                    # Go over all entries in the list and create an equal length list of dicts.
                    for src_entry in source:

                        if isinstance(source, basestring):
                            target = os.path.basename(src_entry),
                        elif isinstance(source, ru.Url):
                            target = os.path.basename(src_entry.path),
                        else:
                            raise Exception("Source %s is neither a string nor a Url (%s)!" %
                                             (source, type(source)))

                        new_sd = {'uid':      ru.generate_id('sd'),
                                  'source':   src_entry,
                                  'target':   target,
                                  'action':   action,
                                  'flags':    flags,
                                  'priority': priority
                        }
                        new_sds.append(new_sd)

                log.debug("Converting list '%s' into dicts '%s'" % (source, new_sds))

                # Add the content of the local list to global list
                ret.extend(new_sds)

            else:
                raise Exception("Source %s is neither an entry nor a list (%s)!" %
                                (source, type(source)))

        else:
            raise Exception("Unknown type of staging directive: %s (%s)" % (sd, type(sd)))

    return ret


# ------------------------------------------------------------------------------

