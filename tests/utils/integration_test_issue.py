#!/usr/bin/env python3

import os
import sys

from github import Github
from datetime import date


# ------------------------------------------------------------------------------
#
token = os.getenv('GIT_TOKEN', '...')
g     = Github(token)
repo  = g.get_repo("radical-cybertools/radical.pilot")

if len(sys.argv) < 3:
    raise ValueError('usage: %s <resource> <test_output>' % sys.argv[0])

with open(sys.argv[2]) as fin:

    title = "Integration Tests failed on %s : %s" % (sys.argv[1], date.today())
    body  = '```python\n' + f.read() + '\n```'

    repo.create_issue(title=title,
                      body=body,
                      labels=[repo.get_label(name="topic:testing"),
                              repo.get_label(name="type:bug"),
                              repo.get_label(name="layer:rp")])


# ------------------------------------------------------------------------------

