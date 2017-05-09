
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
    call expand_staging_directive for the 'input_staging' and 'output_staging'
    elements of the given description
    """

    input_directives  = expand_staging_directives(descr.get('input_staging' ))
    output_directives = expand_staging_directives(descr.get('output_staging'))

    # we now have full sd dicts.  Make sure that we have absolute path
    # for the relevant local targets and sources.
    for sd in input_directives:
        sd['source'] = make_abs_url(sd['source'])

    for sd in output_directives:
        sd['target'] = make_abs_url(sd['target'])

    descr['input_staging']  = input_directives
    descr['output_staging'] = output_directives


# ------------------------------------------------------------------------------
def make_abs_url(path):
    """
    Some paths in data staging directives are to be interpreted as relative to
    the applications working directory.  This specifically holds for *input
    staging sources* and *output staging targets*, at least in those cases where
    they are aspecified as relative path, ie. not as an URL with schema and/or
    host element.

    This helper is testing exactly that condition.  If it applies, it converts
    the path to an absolute URL with `file://localhost/$PWD/` root.  It returns
    that Url as a string, so that it can be readily placed back into the staging
    directives (which are strings at that point).
    """

    if path.startswith('/'): is_abs = True
    else                   : is_abs = False

    url = ru.Url(path)

    if url.schema in [None, '', 'file'     ] and \
       url.host   in [None, '', 'localhost'] :
        url.schema = 'file'
        url.host   = 'localhost'
        if is_abs: url.path = path
        else     : url.path = '%s/%s' % (os.getcwd(), path)

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

    # Convert single entries into a list
    if not isinstance(staging_directives, list):
        staging_directives = [staging_directives]

    # Use this to collect the return value
    new_staging_directive = []

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
            new_staging_directive.append(new_sd)

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
                new_staging_directive.append(new_sd)
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
                new_staging_directive.extend(new_sds)

            else:
                raise Exception("Source %s is neither an entry nor a list (%s)!" %
                                (source, type(source)))

        else:
            raise Exception("Unknown type of staging directive: %s (%s)" % (sd, type(sd)))

    return new_staging_directive


# ------------------------------------------------------------------------------

