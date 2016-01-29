

import os
import saga

from .utils     import logger
from .constants import *

# The Staging Directives are specified using a dict in the following form:
#   staging_directive = {
#       'source':   None, # saga.Url() or string
#       'target':   None, # saga.Url() or string
#       'action':   None, # See 'Action operators' below
#       'flags':    None, # See 'Flags' below
#       'priority': 0     # Control ordering of actions
#   }

#
# Action operators
#
#-----------------------------------------------------------------------------
#
def expand_staging_directive(staging_directive):
    """Take an abbreviated or compressed staging directive and expand it.

    """

    # Convert single entries into a list
    if not isinstance(staging_directive, list):
        staging_directive = [staging_directive]

    # Use this to collect the return value
    new_staging_directive = []

    # We loop over the list of staging directives
    for sd in staging_directive:

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
                logger.warn("append mode on staging not supported (ignored)")

            new_sd = {'source':   src.strip(),
                      'target':   tgt.strip(),
                      'action':   DEFAULT_ACTION,
                      'flags':    DEFAULT_FLAGS,
                      'priority': DEFAULT_PRIORITY
            }
            logger.debug("Converting string '%s' into dict '%s'" % (sd, new_sd))
            new_staging_directive.append(new_sd)

        elif isinstance(sd, dict):
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

            if isinstance(source, basestring) or isinstance(source, saga.Url):

                if target:
                    # Detect asymmetry in source and target length
                    if isinstance(target, list):
                        raise Exception("Source is singular but target is a list")
                else:
                    # We had no target specified, assume the basename of source
                    if isinstance(source, basestring):
                        target = os.path.basename(source)
                    elif isinstance(source, saga.Url):
                        target = os.path.basename(source.path)
                    else:
                        raise Exception("Source %s is neither a string nor a Url (%s)!" %
                                        (source, type(source)))

                # This is a regular entry, complete and append it
                new_sd = {'source':   source,
                          'target':   target,
                          'action':   action,
                          'flags':    DEFAULT_FLAGS,
                          'priority': DEFAULT_PRIORITY
                }
                new_staging_directive.append(new_sd)
                logger.debug("Completing entry '%s'" % new_sd)

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

                        new_sd = {'source':   src_entry,
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
                        elif isinstance(source, saga.Url):
                            target = os.path.basename(src_entry.path),
                        else:
                            raise Exception("Source %s is neither a string nor a Url (%s)!" %
                                             (source, type(source)))

                        new_sd = {'source':   src_entry,
                                  'target':   target,
                                  'action':   action,
                                  'flags':    flags,
                                  'priority': priority
                        }
                        new_sds.append(new_sd)

                logger.debug("Converting list '%s' into dicts '%s'" % (source, new_sds))

                # Add the content of the local list to global list
                new_staging_directive.extend(new_sds)

            else:
                raise Exception("Source %s is neither an entry nor a list (%s)!" %
                                (source, type(source)))

        else:
            raise Exception("Unknown type of staging directive: %s (%s)" % (sd, type(sd)))

    return new_staging_directive
