
import os
import glob
import tarfile

import radical.utils as ru

from .. import constants as rpc

from .staging_helper import StagingHelper


# ------------------------------------------------------------------------------
#
def fetch_filetype(ext, name, sid, src=None, tgt=None, access=None,
        skip_existing=False, fetch_client=False, log=None, rep=None):
    '''
    Args:

        - ext  (str): file extension to fetch
        - name (str): full name of filetype for log messages etc
        - sid  (str): session for which all files are fetched
        - src  (str): dir to look for client session files (`$src/$sid/*.ext`)
        - tgt  (str): dir to store the files in (`$tgt/$sid/*.ext`,
            `$tgt/$sid/$pid/*.ext`)

    Returns:

        list[str]: list of file names (fetched and/or cached)

    '''

    if not log:
        log = ru.Logger('radical.pilot.utils')

    ret = list()

    if not src:
        src = os.getcwd()

    if not tgt:
        tgt = os.getcwd()

    # no point in fetching client files into client sandbox
    if src == tgt:
        fetch_client = False

    if not tgt.startswith('/') and '://' not in tgt:
        tgt = "%s/%s" % (os.getcwd(), tgt)

    # we always create a session dir as real target
    tgt_url = ru.Url("%s/%s/" % (tgt, sid))

    # turn URLs without `schema://host` into `file://localhost`,
    # so that they dont become interpreted as relative paths.
    if not tgt_url.schema: tgt_url.schema = 'file'
    if not tgt_url.host  : tgt_url.host   = 'localhost'

    # create target dir for session
    ru.rec_makedir(tgt_url.path)

    # first fetch client files
    if fetch_client:
        client_files = glob.glob("%s/%s/**.%s" % (src, sid, ext))
        if not client_files:
            raise RuntimeError('no client %s in %s/%s' % (name, src, sid))

        for client_file in client_files:

            ftgt = ru.Url('%s/%s' % (tgt_url, os.path.basename(client_file)))
            ret.append("%s" % ftgt.path)

            if skip_existing and os.path.isfile(ftgt.path) \
                             and os.path.getsize(ftgt.path):
                pass
            else:
                log.debug('fetch client file %s', client_file)
                stager = StagingHelper(log)
                stager.copy(client_file, ftgt, flags=rpc.CREATE_PARENTS)

    # we need the session json for pilot details
    pilots = list()
    for fname in glob.glob('%s/pmgr.*.json' % sid):
        json_doc = ru.read_json(fname)
        pilots.extend(json_doc['pilots'])

    num_pilots = len(pilots)

    log.debug("Session: %s", sid)
    log.debug("Number of pilots in session: %d", num_pilots)

    for pilot in pilots:

        pid = pilot['uid']

        # create target dir for this pilot
        ru.rec_makedir('%s/%s' % (tgt_url.path, pid))

        try:
            log.debug("processing pilot '%s'", pid)

            sandbox_url = ru.Url(pilot['pilot_sandbox'])

            if access:
                # Allow to use a different access schema than used for the the
                # run.  Useful if you ran from the headnode, but would like to
                # retrieve the files to your desktop (Hello Titan).
                access_url = ru.Url(access)
                sandbox_url.schema = access_url.schema
                sandbox_url.host   = access_url.host

            stager = StagingHelper(log)

            # Try to fetch a tarball of files, so that we can get them
            # all in one go!
            tarball_name  = '%s.%s.tgz'   % (pid, ext)
            tarball_tgt   = '%s/%s/%s/%s' % (tgt, sid, pid, tarball_name)
            tarball_local = False

            # check if we have a local tarball already
            if skip_existing and \
               os.path.isfile(tarball_tgt) and \
               os.path.getsize(tarball_tgt):
                tarball_local = True

            if not tarball_local:
                # need to fetch tarball
                #  - if no remote tarball exists, create it
                #  - fetch remote tarball

                tarball_remote = True
                log.info("fetch '%s%s' to '%s'.", sandbox_url,
                         tarball_name, tgt_url)

                rs_file = "%s%s" % (sandbox_url, tarball_name)
                stager.copy(rs_file, tarball_tgt, flags=rpc.CREATE_PARENTS)

                if os.path.getsize(tarball_tgt) == 0:
                    tarball_remote = False

            if not tarball_remote:
                raise RuntimeError('no tarball for %s' % pid)

            # we now have a local tarball - unpack it
            # note that we do not check if it was unpacked before - it's simpler
            # (and possibly cheaper) to just do that again
            log.info('Extract tarball %s', tarball_tgt)
            tarball = tarfile.open(tarball_tgt, mode='r:gz')
            tarball.extractall("%s/%s" % (tgt_url.path, pid))

            files = glob.glob("%s/%s/**.%s" % (tgt_url.path, pid, ext))
            ret.extend(files)

            if rep:
                rep.ok("+ %s (%s)\n" % (pid, name))

        except Exception:
            # do not raise, we still try the other pilots
            log.exception('failed to fetch %s for %s', pid, name)
            if rep:
                rep.error("- %s (%s)\n" % (pid, name))

    return ret


# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, src=None, tgt=None, access=None,
        skip_existing=False, fetch_client=False, log=None, rep=None):

    return fetch_filetype('prof', 'profiles', sid, src, tgt, access,
            skip_existing, fetch_client, log, rep)


# ------------------------------------------------------------------------------
#
def fetch_logfiles (sid, src=None, tgt=None, access=None,
        skip_existing=False, fetch_client=False, log=None, rep=None):

    return fetch_filetype('log', 'logfiles', sid, src, tgt, access,
            skip_existing, fetch_client, log, rep)


# ------------------------------------------------------------------------------

