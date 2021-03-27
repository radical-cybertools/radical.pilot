
import radical.utils as ru

import .constants as rpc


class StagingDirective(ru.Description):

    _schema = {
            rpc.MODE   : str,
            rpc.UID    : str,
            rpc.SOURCE : str,
            rpc.TARGET : str,
            rpc.ACTION : str,
            rpc.FLAGS  : int,
            rpc.PROF_ID: str,
    }


    _defaults =
            rpc.MODE   : None,
            rpc.UID    : None,
            rpc.SOURCE : None,
            rpc.TARGET : None,
            rpc.ACTION : rpc.TRANSFER,
            rpc.FLAGS  : None,
            rpc.PROF_ID: None,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, mode, from_dict=None):

        assert(mode in [IN, OUT])

        if isinstance(from_dict, str):

            # We detected a string, convert into dict.  The interpretation
            # differs depending of redirection character being present in the
            # string.

            sd = from_dict

            if   '>'  in sd: src, tgt = sd.split('>' , 2)
            elif '<'  in sd: tgt, src = sd.split('<' , 2)
            else           : src, tgt = sd, os.path.basename(ru.Url(sd).path)

            from_dict = {'source': src.strip(),
                         'target': tgt.strip(),
                         'action': DEFAULT_ACTION,
                         'flags' : DEFAULT_FLAGS,

        ru.Description.__init__(self, from_dict=from_dict)

        self.mode = mode
        self.uid  = ru.generate_id('sd.%(item_counter)06d', ru.ID_CUSTOM,
                                                            ns=session.uid)


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        # TODO: should make sure that expansion happened
        pass


    # --------------------------------------------------------------------------
    #
    def expand(self, session, context):

  # def expand_sds(sds, src_ctx, tgt_ctx, session):
  #     '''
  #     Take an abbreviated or compressed staging directive and fill in all details
  #     needed to actually perform the data transfer (or whateve the SD's `action`
  #     is).
  #
  #     A staging directive is a description of a specific data staging operation to
  #     be performed by RP.  It can have the following keys:
  #     '''

        '''
        convert any simple, string based staging directive in the description
        into its dictionary equivalent, and expand all file specs to full URLs.
        the latter expansion is always specific to

          - the the task to which this staging dorective belongs
          - the pilot on which that task is scheduled
          - the session to which that pilot belongs
          - the resource on which the pilot is executed

        Directives can refer to sandboxes of other tasks and pilots by
        specifying the respective UIDs.

        Expansion may not always be possible, for example a pilot sandbox can
        only be fully expanded once the task has in fact been scheduled onto
        specific pilot; a specific task sandbox can only be expanded once that
        task has been scheduled on a pkilot, and that pilot has been scheduled
        onto a resource.  For those information, the method will refer to the
        `context` dictionary and expects to find the expanded sandboxes
        registered under the respective schema.  The `session` argument is used
        to derive the sandbox locations which are not listed in the `context`,
        if possible.  This method will raise a ValueError if expansion is not
        possible due to incomplete information.


        The following types of expansions are performed:

          - short form (str) to long form (dict)

            in : ['input.dat']
            out: {'source' : 'sandbox://client/input.dat',
                  'target' : 'sandbox://task/input.dat',
                  'action' : rp.TRANSFER}

            in : ['input.dat > staged.dat']
            out: {'source' : 'sandbox://client/input.dat',
                  'target' : 'sandbox://task/staged.dat',
                  'action' : rp.TRANSFER}

          - sandbox expansion

            in : {'source' : 'sandbox://client/input.dat',
                  'target' : 'sandbox://task.0000/staged.dat',
                  'action' : rp.TRANSFER}
            out: {'source' : 'file:///tmp/input.dat',
                  'target' : 'sftp://host/tmp/pilot.0000/task.0000/staged.dat',
                  'action' : rp.TRANSFER}

        The following special sandbox URL schemas are interpreted:

          client:///     - the application workdir on the client host
          resource:///   - the RP resource sandbox on the remote host
          session:///    - the RP session  sandbox on the remote host
          pilot:///      - the RP pilot    sandbox on the remote host
                           (pilot hosts the task)
          pilot.0001:/// - the RP resource sandbox on the remote host
                           (pilot may not host the task)
          task:///       - the RP resource sandbox on the remote host
                           (this task)
          task.0002:///  - the RP resource sandbox on the remote host
                           (any task)
        '''

        # FIXME: expand sandboxes
        assert(sd.get('context'))  # needed for sandboxes

        assert(isinstance(sd, dict)):

        # sanity check on dict syntax
        valid_keys = ['source', 'target', 'action', 'flags', 'uid', 'prof_id']
        for k in sd:
            if k not in valid_keys:
                raise ValueError('"%s" is invalid on staging directive' % k)

        src = sd.get('source')
        tgt = sd.get('target',   os.path.basename(ru.Url(source).path))

        assert(src)

        if not src:
            raise Exception("Staging directive dict has no source member!")

        # FIXME: ns = session ID
        self.source = src
        self.target = tgt


# ------------------------------------------------------------------------------
#
class StagingInputDirective(StagingDirective):

    def __init__(self, from_dict=None):

        StagingDirective.__init__(self, mode=rpc.IN, from_dict=from_dict)


# ------------------------------------------------------------------------------
#
class StagingOutputDirective(StagingDirective):

    def __init__(self, from_dict=None):

        StagingDirective.__init__(self, mode=rpc.OUT, from_dict=from_dict)



# ------------------------------------------------------------------------------
#
def complete_url(path, contexts, log=None):
    '''
    Some paths in data staging directives are to be interpreted relative to
    certain locations, namely relative to

        * `sandbox://client/`  : the client's working directory
        * `sandbox://resource/`: the RP    sandbox on the target resource
        * `sandbox://pilot/`   : the pilot sandbox on the target resource
        * `sandbox://task/`    : the task  sandbox on the target resource
        * `sandbox://<uid>/`   : the task  sandbox for task with given uid

    The `context` parameter is expected to be a dict which provides a set of
    URLs to be used to expand the path:

        {
          'client': 'file://localhost/foo/bar',
          'pilot' : 'sftp://host.net/tmp/session.0000/pilot.000',
          'pwd'   : '/tmp/session.0000',
          ...
        }

    URL schemas other than `sandbox://` are left alone, values without URL
    schema are interpreted as path (either absolute or relative with respect to
    `pwd`)

    The method returns an instance of ru.Url.  If an expansion is not possible,
    the return value is None.

    Note that URL parsing is not really cheap, so this method should be used
    conservatively.
    '''

    assert(ru.is_string(path) and path)

    # We assume that the user knows what she is doing when using absolute paths,
    # and make no attempts to verify those.
    if str(path)[0] == '/':
        if log: log.error('abs path')
        return ru.Url('file:///' + path), None

    # we always want a schema, otherwise interpret as path relative to `pwd`
    if '://' not in path:
        # no schema: path relative to `pwd`
        for context in contexts:
            if 'pwd' in context:
                if log: log.error('pwd path')
                return ru.Url(context['pwd'] + '/' + path), None

    # only expand sandbox schemas
    if not path.startswith('sandbox://'):
        if log: log.error('norm path')
        return ru.Url(path), None

    _, _, host, rest = path.split('/', 3)
    assert(host)
    assert(rest)

    # backward compatibility
    if host == 'unit': host = 'task'

    # need to do sandbox expansion
    for context in contexts:
        if host in context:
            if log: log.error('comp path')
            return ru.Url(context[host] + '/' + rest), None

    # cannot expand: this likely refers to an unknown UID
    if log: log.error('oops path: %s', host)
    return [None, host]



# ------------------------------------------------------------------------------

