---
name: Bug report
about: Report a bug in RADICAL-Pilot
title: ''
labels: layer:rp, type:bug
assignees: ''

---

When encountering an issue during the execution of a RADICAL-Pilot (RP) application, please check whether the source of the error is in the application code or in the code executed by the compute units (i.e., executable). If you suspect that RP is the source of the error, please open a ticket at https://github.com/radical-cybertools/radical.pilot/issues, following these steps:

1. **Enable verbose messages**: Run your application script again, setting the ``RADICAL_VERBOSE=DEBUG`` and ``RADICAL_PILOT_VERBOSE=DEBUG`` environment variables. By default, RP redirects debug messages to Standard Error but you may want to redirect those messages to a single file. For example, with bash: `RADICAL_VERBOSE=DEBUG RADICAL_PILOT_VERBOSE=DEBUG python example.py &> debug.out`.

2. **Client and remote logs in RP**: RP creates multiple logs files in a client-side sandbox and a server-side sandbox. The client-side sandbox is created in the
working directory on the client machine (where you launched your application script); the server-side sandbox is created on the remote machine (HPC) in a predefined location. You can collect all the logs by running the following command on the client machine: `radical-pilot-fecth-logfiles <session id>`. In order to determine the session id, you can look in the debug logs or for a folder that is created in the directory from which you launched the application script on the client machine. That directory should have the format ```rp.session.*```. You can find the latest folder by doing ``ls -ltr`` (last is recent). The `radical-pilot-fecth-logfiles` command collects all the logfiles to that `rp.session.*` folder. Please tar and (b/g)zip that folder and attach it to the github ticket.

3. **Provide information about the error**: After fetching all the log files, go in the `rp.session.*` folder and execute `grep -rl ERROR .`. Please include the output of that command in the ticket.
