
#
# Action operators
#
COPY     = 'Copy'     # local cp
LINK     = 'Link'     # local ln -s
MOVE     = 'Move'     # local mv
TRANSFER = 'Transfer' # saga remote transfer TODO: This might just be a special case of copy

#
# Flags
#
SKIP_FAILED    = 'SkipFailed'     # Don't stage out files if tasks failed
CREATE_PARENTS = 'CreateParents'  # Create parent directories while writing to staging area


class StagingDirectives(dict):

    def __init__(self):

        # TODO: Convert to attrib iface
        self.source   = None # radical.pilot.Url() or string
        self.target   = None # radical.pilot.Url() or string
        self.action   = None # COPY, LINK, MOVE, TRANSFER
        self.flags    = None # SKIP_FAILED, CREATE_PARENTS
        self.priority = 0    # Control ordering of actions, mainly interesting for output probably

    #------------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        obj_dict = {
            'source'   : self.source,
            'target'   : self.target,
            'action'   : self.action,
            'flags'    : self.flags,
            'priority' : self.priority,
        }
        return obj_dict


    #------------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())
