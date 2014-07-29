# staging_directive = {
#     'source':   None, # radical.pilot.Url() or string
#     'target':   None, # radical.pilot.Url() or string
#     'action':   None, # COPY, LINK, MOVE, TRANSFER
#     'flags':    None, # SKIP_FAILED, CREATE_PARENTS
#     'priority': 0    # Control ordering of actions, mainly interesting for output probably
# }

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
