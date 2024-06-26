
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

    files = list()

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
    tgt_url = ru.Url("%s/%s" % (tgt, sid))

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
            files.append("%s" % ftgt.path)

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

        pid      = pilot['uid']
        tar_name = '%s.%s.tgz' % (pid, ext)

        # create target dir for this pilot
        ru.rec_makedir('%s/%s' % (tgt_url.path, pid))

        try:
            log.debug("processing pilot '%s'", pid)

            if access:
                # Allow to use a different access schema than used for the the
                # run.  Useful if you ran from the headnode, but would like to
                # retrieve the files to your desktop (Hello Titan).
                access_url = ru.Url(access)
                sandbox_url.schema = access_url.schema
                sandbox_url.host   = access_url.host

            else:
                sandbox_url = ru.Url(pilot['pilot_sandbox'])

            sandbox_url.path.rstrip('/')

            src_url  = ru.Url('%s/%s' % (sandbox_url, tar_name))
            src_dir  = os.path.dirname(src_url.path)
            is_local = False

            log.debug("sandbox: %s", sandbox_url)
            log.debug("src_url: %s", src_url)

            # check if we have a local tarball already
            if skip_existing and \
               os.path.isfile(tgt_url.path) and \
               os.path.getsize(tgt_url.path):
                is_local = True

            stager = StagingHelper(log)

            if not is_local:
                # need to fetch tarball
                #  - if no remote tarball exists, create it
                #  - fetch remote tarball
                _, _, ret = stager.sh_callout(src_url, 'test -f %s' % src_url.path)

                if ret:
                    # no tarball on remote side, create one
                    #
                    find_cmd = "find . -name '*.%s'" % ext
                    tar_cmd  = "cd %s && tar czf %s $(%s)" \
                             % (src_dir, tar_name, find_cmd)

                    out, err, ret = stager.sh_callout(src_url, tar_cmd)
                    log.debug("create with '%s': %s/%s", tar_cmd, out, err)

                    if ret:
                        raise RuntimeError("failed to create tarball: %s" % err)

                log.info("fetch '%s' to '%s'.", src_url, tgt_url)
                stager.copy(src_url, tgt_url, flags=rpc.CREATE_PARENTS)

            # we now have a local tarball - unpack it
            # note that we do not check if it was unpacked before - it's simpler
            # (and possibly cheaper) to just do that again
            log.info('Extract tarball %s', tgt_url.path)
            tarball = tarfile.open('%s/%s' % (tgt_url.path, tar_name), mode='r:gz')
            tarball.extractall("%s/%s" % (tgt_url.path, pid))

            found = glob.glob("%s/%s/*.%s" % (tgt_url.path, pid, ext))
            files.extend(found)

            if rep:
                rep.ok("+ %s (%s)\n" % (pid, name))

        except Exception:
            # do not raise, we still try the other pilots
            log.exception('failed to fetch %s for %s', pid, name)
            if rep:
                rep.error("- %s (%s)\n" % (pid, name))

    return files


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

