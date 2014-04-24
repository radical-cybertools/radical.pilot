
#
# Action operators
#
COPY     = 'Copy'     # local cp
LINK     = 'Link'     # local ln -s
MOVE     = 'Move'     # local mv
TRANSFER = 'Transfer' # saga remote transfer

#
# Flags
#
SKIP_FAILED    = 'SkipFailed'     # Don't stage out files if tasks failed
CREATE_PARENTS = 'CreateParents'  # Create parent directories while writing to staging area


class StagingDirectives(saga.Attributes):

    def __init__(self):

        # TODO: Convert to attrib iface
        self.source   = None # radical.pilot.Url() or string
        self.target   = None # radical.pilot.Url() or string
        self.action   = None # COPY, LINK, MOVE, TRANSFER
        self.flags    = None # SKIP_FAILED, CREATE_PARENTS
        self.priority = 0    # Control ordering of actions
