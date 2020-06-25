
import re
import os
import pprint

# we know that some env vars are not worth preserving.  We explicitly exclude
# those which are common to have complex syntax and need serious caution on
# shell escaping:
BLACKLIST  = ['PS1', 'LS_COLORS', '_']

# identical task `pre_exec_env` settings will result in the same environment
# settings, so we cache those environments here.  We rely on a hash to ensure
# `pre_exec_env` identity.  Not that this assumes that settings do not depend
# on, say, the unit ID or similar, which needs very clear and prominent
# documentation.  Caching can be turned off by adding a unique noop string to
# the `pre_exec_env` list - but we probably also add a config flag.
_env_cache = dict()


# ------------------------------------------------------------------------------
#
# helper to parse environment from a file: this method parses the output of
# `env` and returns a dict with the found environment settings.
#
def env_read(fname):

    # POSIX definition of variable names
    key_pat = r'^[A-Za-z_][A-Za-z_0-9]*$'
    env     = dict()

    with open(fname, 'r') as fin:

        key = None
        val = ''

        for line in fin.readlines():

            # remove newline
            line = line.rstrip('\n')

            # search for new key
            if '=' not in line:
                # no key present - append linebreak and line to value
                val += '\n'
                val += line
                continue


            this_key, this_val = line.split('=', 1)

            if re.match(key_pat, this_key):
                # valid key - store previous key/val if we have any, and
                # initialize `key` and `val`
                if key and key not in BLACKLIST:
                    env[key] = val

                key = this_key
                val = this_val
            else:
                # invalid key - append linebreak and line to value
                val += '\n'
                val += line

        # store last key/val if we have any
        if key and key not in BLACKLIST:
            env[key] = val

    return env


# ------------------------------------------------------------------------------
#
def env_prep(base, remove=None, pre_exec_env=None, tgt=None):

    if remove       is None: remove       = os.environ
    if pre_exec_env is None: pre_exec_env = list()

    # Write a temporary shell script which
    #
    #   - unsets all variables which are not defined in `base` but are defined
    #     in the `remove` env dict;
    #   - unset all blacklisted vars;
    #   - sets all variables defined in the `base` env dict;
    #   - runs the `pre_exec_env` commands given;
    #   - dumps the resulting env in a temporary file;
    #
    # Then run that command and read the resulting env back into a dict to
    # return.  If `tgt` is specified, then also create a file at the given
    # name and fill it with `unset` and `tgt` statements to recreate that
    # specific environment: any shell sourcing that `tgt` file thus activates
    # the environment thus prepared.
    #
    # FIXME: better tmp file names to avoid collisions
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
            if k not in BLACKLIST:
                fout.write("export %s='%s'\n" % (k, v))
        fout.write('\n')

        fout.write('# pre_exec_env\n')
        for cmd in pre_exec_env:
            fout.write('%s\n' % cmd)
        fout.write('\n')

    os.system('/bin/sh -c ". ./env.sh && env > ./env.tmp"')
    env = env_read('./env.tmp')


    # if `tgt` is specified, create a script with that name which unsets the
    # same names as in the tmp script above, and exports all vars from the
    # resulting env from above
    if tgt:
        with open(tgt, 'w') as fout:

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
    # This is a debug method

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

    return only_1, only_2, changed


# ------------------------------------------------------------------------------

