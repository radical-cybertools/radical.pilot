
#
# Action operators
#
COPY     = 'Copy'     # cp
LINK     = 'Link'     # ln -s
MOVE     = 'Move'     # mv
TRANSFER = 'Transfer' # saga transfer

#
# Temporal instruction
#
BEGIN        = 'Begin'      # Stage data in before beginning of task
END          = 'End'        # Stage data out after task completes
END_SUCCESS  = 'EndSuccess' # Stage data out only if execution was successful

class StagingDirectives(saga.Attributes):

    def __init__(self):

        # TODO: Convert to attrib iface
        self.source   = None # radical.pilot.Url()
        self.target   = None # radical.pilot.Url()
        self.action   = None # COPY, LINK, MOVE, TRANSFER
        self.moment   = None # BEGIN, END, END_SUCCESS
        self.flags    = None # e.g. SAGA file flags, like CREATE, etc.
        self.priority = 0    # Control ordering of actions
