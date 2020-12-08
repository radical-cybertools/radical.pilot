---
name: Bug report
about: Create a report to help us improve
title: ''
labels: layer:rp, type:bug
assignees: ''

---

When encountering an issue during execution, please do due diligence to check whether the source of the error is in the user script/tool. If you suspect the source of the error comes from EnTK or tools below please open a ticket in the github repo and follow these steps:

### Enable verbose messages

Run your script again with ``RADICAL_VERBOSE=DEBUG`` and ``RADICAL_PILOT_VERBOSE=DEBUG``. Once these environment variables are set, a lot of messages will be displayed as they are written to the standard error stream. Please redirect these messages to a single file.

Example:
```
RADICAL_VERBOSE=DEBUG RADICAL_PILOT_VERBOSE=DEBUG python example.py
```

### Client and remote logs in RP

When running a RP script, multiple logs are created by the components of RP. A set of these logs are created in the current working directory on the client machine (where your script lies) and a set of logs are created on the remote machine (HPC) in a specific location. You can bring all the logs to the client by running the following cmd (on the client):

```
radical-pilot-fecth-logfiles <session id>
```

In order to determine the session id, you can look for a folder that is created on the client in current working directory. It should have the format ```rp.session.*```. You can find the latest folder by doing ``ls -ltr`` (last is recent).

All the logfiles are brought to this `rp.session.*` folder. Please zip this folder and attach to github ticket.

### Provide information about the error

After fetching all the log files, go in the `rp.session.*` folder and execute `grep -rl ERROR .` and include in the ticket any error message.