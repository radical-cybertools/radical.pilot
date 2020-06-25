
BLACKLIST="PS1 LS_COLORS _"

# ------------------------------------------------------------------------------
#
env_dump(){

    tgt=$1

    env | sort > $tgt
}


# ------------------------------------------------------------------------------
#
env_prep(){

    base=$1;   shift
    remove=$1; shift
    tgt=$1;    shift
    pre_exec_env="$*"

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
    # return.  If `export` is specified, then also create a file at the given
    # name and fill it with `unset` and `export` statements to recreate that
    # specific environment: any shell sourcing that `export` file thus activates
    # the environment thus prepared.
    #
    # FIXME: better tmp file names to avoid collisions
    #
    # key_pat = r'^[A-Za-z_][A-Za-z_0-9]*$'

    base_keys=$( cat "$base" \
               | sort \
               | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
               | cut -f1 -d=
               )

    if ! test -z "$remove"
    then
        rem_keys=$( cat "$remove" \
                  | sort \
                  | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
                  | cut -f1 -d=
                  )
    fi

    printf "\n# unset\n" > $tgt
    for k in $rem_keys
    do
        test -z "$k" && continue
        grep -e "^$k=" $base >/dev/null || echo "unset '$k'" >> $tgt
    done


    printf "\n# blacklist\n" >> $tgt
    for k in $BLACKLIST
    do
        echo "unset '$k'" >> $tgt
    done


    printf "\n# export\n" >> $tgt
    for k in $base_keys
    do
        test -z "$k" && continue
        if ! expr "$BLACKLIST" : ".*\<$k\>.*" >/dev/null
        then
            bv=$(grep -e "^$k=" $base | cut -f 2- -d= | sed -e 's/"/\\"/g')
            echo "export $k=\"$bv\"" >> $tgt
        fi
    done

    printf "\n# pre_exec_env\n" >> $tgt
    echo "$pre_exec_env" >> $tgt
}


# ------------------------------------------------------------------------------

# export FOO="foo\"bar\"buz"
# env_dump ed1
# export BAR="foo\"bar\"buz"
# env_dump ed2
#
# env_prep ed1 ed2 ed3



