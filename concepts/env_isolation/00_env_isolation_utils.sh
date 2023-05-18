

# The methods defined here reflect the capabilities to dump and prepare
# environments as implemented in `00_env_isolation.py`, but are, obviously, for
# use in shell scripts.  Specifically `05_env_isolation_wrapper.sh` uses these
# methods to temporarily escape the LM env before running `pre_exec_cmd`
# directives.

# do not export blacklisted env variables
BLACKLIST="PS1 LS_COLORS _"


# ------------------------------------------------------------------------------
#
env_dump(){

    # Note that this capture will result in an unquoted dump where the values
    # can contain spaces, quotes (double and single), non-printable characters
    # etc.  The guarantees we have are:
    #
    #   - variable names begin with a letter, and contain letters, numbers and
    #     underscores.  From POSIX:
    #
    #       "Environment variable names used by the utilities in the Shell and
    #       Utilities volume of IEEE Std 1003.1-2001 consist solely of uppercase
    #       letters, digits, and the '_' (underscore) from the characters
    #       defined in Portable Character Set and do not begin with a digit."
    #
    #     Note that implementations usually also support lowercase letters, so
    #     we'll have to support that, too (event though it is rarely used for
    #     exported system variables).
    #
    #   - variable values can have any character.  Again POSIX:
    #
    #       "For values to be portable across systems conforming to IEEE Std
    #       1003.1-2001, the value shall be composed of characters from the
    #       portable character set (except NUL [...])."
    #
    # So the rules for names are strict, for values they are, unfortunately,
    # loose.  Specifically, values can contain unprintable characters and also
    # newlines.  While the Python equivalent of `env_prep` handles that case
    # well, the shell implementation below will simply ignore any lines which do
    # not start with a valid key.

    tgt=$1; shift

    env | sort > $tgt
}


# ------------------------------------------------------------------------------
#
env_prep(){

    # Write a shell script to `tgt` which
    #
    #   - unsets all variables which are not defined in `base` but are defined
    #     in the `remove` env dict;
    #   - unset all blacklisted vars;
    #   - sets all variables defined in the `base` env dict;
    #   - runs the `pre_exec_env` commands given;
    #
    # The resulting shell script can be sourced to activate the resulting
    # environment.  Note that, other than the Python counterpart, this method
    # does not return any representation of the resulting environment, but
    # simply creates the described shell script.


    base=$1;   shift
    remove=$1; shift
    tgt=$1;    shift
    # all remaining args are interpreted as `pre_exec_env`


    # get keys from `base` environment dump
    base_keys=$( cat "$base" \
               | sort \
               | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
               | cut -f1 -d=
               )

    if ! test -z "$remove"
    then
        # get keys from `remove` environment dump
        rem_keys=$( cat "$remove" \
                  | sort \
                  | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
                  | cut -f1 -d=
                  )
    fi

    # unset all keys which are in `remove` but not in `base`
    printf "\n# unset\n" > $tgt
    for k in $rem_keys
    do
        grep -e "^$k=" $base >/dev/null || echo "unset '$k'" >> $tgt
    done


    # unset all blacklisted keys
    printf "\n# blacklist\n" >> $tgt
    for k in $BLACKLIST
    do
        echo "unset '$k'" >> $tgt
    done


    # export all keys from `base`
    printf "\n# export\n" >> $tgt
    for k in $base_keys
    do
        # exclude blacklisted keys
        if ! expr "$BLACKLIST" : ".*\<$k\>.*" >/dev/null
        then
            bv=$(grep -e "^$k=" $base | cut -f 2- -d= | sed -e 's/"/\\"/g')
            echo "export $k=\"$bv\"" >> $tgt
        fi
    done

    # run all remaining arguments as `pre_exec` commands
    printf "\n# pre_exec_env\n" >> $tgt
    for pe in "$@"
    do
        echo "$pe" >> $tgt
    done
}


# ------------------------------------------------------------------------------

# # example usage
# export FOO="foo\"bar\"buz"
# env_dump ed1.env
#
# export BAR="foo\"bar\"buz"
# env_dump ed2.env
#
# env_prep ed1.env ed2.env ed3.sh  "echo foo bar" "echo buz"

