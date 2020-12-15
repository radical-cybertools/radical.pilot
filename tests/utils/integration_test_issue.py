from github import Github
import os
import sys
from datetime import date

token = os.getenv('GIT_TOKEN', '...')
g = Github(token)
repo = g.get_repo("radical-cybertools/radical.pilot")

with open(sys.argv[1]) as f:
    lines = f.readlines()

i = repo.create_issue(
    title="Integration Tests failed on SDSC Comet: %s" % date.today(),
    body='```python\n' + ''.join(x for x in lines) + '\n```',
    labels=[repo.get_label(name="topic:testing"),
            repo.get_label(name="type:bug"),
            repo.get_label(name="layer:rp")])
