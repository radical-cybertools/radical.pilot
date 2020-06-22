
import re
import os
import pprint

BLACKLIST = ['PS1', 'LS_COLORS']

# ------------------------------------------------------------------------------
#
# helper to parse environment from a file
#
def env_read(fname):
    key_pat = r'^[A-Za-z_][A-Za-z_0-9]*$'
    env     = dict()
    with open(fname, 'r') as fin:

        key = None
        val = ''
        for line in fin.readlines():

            # remove newline
            line = line.rstrip('\n')

            # search for new key
            key, val = line.split('=', 1)
            key_ok = False

            if re.match(key_pat, key):
                # valid key - store previous key/val if we have any
                if key and key not in BLACKLIST:
                    env[key] = val
            else:
                # invalid key - append linebreak and line to previous value
                val += '\n'
                val += line

        # store last key/val if we have any
        if key:
            env[key] = val

    return env


# ------------------------------------------------------------------------------
#
def env_prep(base, remove=None, pre_exec=None, dump=None):

    if remove   is None: remove   = os.environ
    if pre_exec is None: pre_exec = list()

    # Write a temporary shell script which
    #
    #   - unsets all variables which are not defined in `base` but are defined
    #     in the `remove` env dict;
    #   - unset all blacklisted vars
    #   - sets all variables defined in the `base` end dict;
    #   - runs the `pre_exec` commands given;
    #   - dumps the resulting env in a temporary file;
    #   - dumps the env also in `dump`, if specified.
    #
    # Then run that command and read the resulting env back into a dict to
    # return.
    #

    with open('./env.sh', 'w') as fout:

        fout.write('\n# unset\n')
        for k in remove:
            if k not in base:
                fout.write('unset %s\n' % k)
        fout.write('\n')

        fout.write('# blacklist\n')
        for k in BLACKLIST:
            fout.write('unset %s\n' % k)
        fout.write('\n')

        fout.write('# export\n')
        for k, v in base.items():
            # FIXME: shell quoting for value
            fout.write("export %s='%s'\n" % (k, v))
        fout.write('\n')

        fout.write('# pre_exec\n')
        for cmd in pre_exec:
            fout.write('%s\n' % cmd)
        fout.write('\n')

    os.system('/bin/sh -c ". ./env.sh && env > ./env.tmp"')
    env = env_read('./env.tmp')


    # if dump is specified, create a script with that name which unsets the same
    # names as in the tmp script above, and exports all vars from the resulting
    # env from above
    if dump:
        with open(dump, 'w') as fout:

            fout.write('\n# unset\n')
            for k in remove:
                if k not in base:
                    fout.write('unset %s\n' % k)
            fout.write('\n')

            fout.write('# blacklist\n')
            for k in BLACKLIST:
                fout.write('unset %s\n' % k)
            fout.write('\n')

            fout.write('# export\n')
            for k, v in env.items():
                # FIXME: shell quoting for value
                fout.write("export %s='%s'\n" % (k, v))
            fout.write('\n')

    return env


# ------------------------------------------------------------------------------
#
def env_diff(env_1, env_2):

    # find all keys that changed in any way, or appeared / disappeared
    only_1  = dict()
    only_2  = dict()
    changed = dict()

    keys_1 = sorted(env_1.keys())
    keys_2 = sorted(env_2.keys())

    for k in keys_1:
        v = env_1[k]
        if   k not in env_2: only_1[k]  = v
        elif v != env_2[k] : changed[k] = [v, env_2[k]]

    for k in keys_2:
        v = env_2[k]
        if   k not in env_1: only_2[k]  = v
        elif v != env_1[k] : changed[k] = [env_1[k], v]

    print('------------- only env_1')
    pprint.pprint(only_1)
    print('------------- only env_2')
    pprint.pprint(only_2)
    print('------------- changed')
    pprint.pprint(changed)


# ------------------------------------------------------------------------------

