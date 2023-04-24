
import os
import glob
import tarfile

import radical.saga  as rs
import radical.utils as ru

from   .db_utils import get_session_docs

rs_fs = rs.filesystem


# ------------------------------------------------------------------------------
#
def fetch_json(sid, dburl=None, tgt=None, skip_existing=False, session=None,
               log=None):
    '''
    returns file name
    '''

    if not log and session:
        log = session._log
    elif not log:
        log = ru.Logger('radical.pilot.utils')

    if session:
        rep = session._rep
    else:
        rep = ru.Reporter('radical.pilot.utils')

    if not tgt:
        tgt = os.getcwd()

    if tgt.startswith('/'):
        dst = '%s/%s/%s.json' % (tgt, sid, sid)
    else:
        dst = '%s/%s/%s/%s.json' % (os.getcwd(), tgt, sid, sid)

    ru.rec_makedir(os.path.dirname(dst))

    if skip_existing and os.path.isfile(dst) and os.path.getsize(dst):
        log.info("session already in %s", dst)
        return dst

    # need to fetch from MongoDB
    if not dburl:
        dburl = os.environ.get('RADICAL_PILOT_DBURL')

    if not dburl:
        raise ValueError('need RADICAL_PILOT_DBURL to fetch session')

    mongo, db, _, _, _ = ru.mongodb_connect(dburl)

    json_docs = get_session_docs(sid, db)
    ru.write_json(json_docs, dst)
    mongo.close()

    log.info("session written to %s", dst)
    rep.ok("+ %s (json)\n" % sid)

    return dst


# ------------------------------------------------------------------------------
#
def fetch_filetype(ext, name, sid, dburl=None, src=None, tgt=None, access=None,
        session=None, skip_existing=False, fetch_client=False, log=None):
    '''
    Args:
        ext  (str): file extension to fetch
        name (str): full name of filetype for log messages etc
        sid  (str): session for which all files are fetched
        src  (str): dir to look for client session files (`$src/$sid/*.ext`)
        tgt  (str): dir to store the files in (`$tgt/$sid/*.ext`,
            `$tgt/$sid/$pid/*.ext`)

    Returns:
        list[str]: list of file names (fetched and/or cached)
    '''

    if not log and session:
        log = session._log

    elif not log:
        log = ru.Logger('radical.pilot.utils')

    if session:
        rep = session._rep
    else:
        rep = ru.Reporter('radical.pilot.utils')

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
    tgt_url = rs.Url("%s/%s/" % (tgt, sid))

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

            ftgt = rs.Url('%s/%s' % (tgt_url, os.path.basename(client_file)))
            ret.append("%s" % ftgt.path)

            if skip_existing and os.path.isfile(ftgt.path) \
                             and os.path.getsize(ftgt.path):
                pass
            else:
                log.debug('fetch client file %s', client_file)
                rs_file = rs_fs.File(client_file, session=session)
                rs_file.copy(ftgt, flags=rs_fs.CREATE_PARENTS)
                rs_file.close()

    # we need the session json for pilot details
    json_name  = fetch_json(sid, dburl, tgt, skip_existing, session, log)
    json_docs  = ru.read_json(json_name)
    pilots     = json_docs['pilot']
    num_pilots = len(pilots)

    log.debug("Session: %s", sid)
    log.debug("Number of pilots in session: %d", num_pilots)

    for pilot in pilots:

        pid = pilot['uid']

        # create target dir for this pilot
        ru.rec_makedir('%s/%s' % (tgt_url.path, pid))

        try:
            log.debug("processing pilot '%s'", pid)

            sandbox_url = rs.Url(pilot['pilot_sandbox'])

            if access:
                # Allow to use a different access schema than used for the the
                # run.  Useful if you ran from the headnode, but would like to
                # retrieve the files to your desktop (Hello Titan).
                access_url = rs.Url(access)
                sandbox_url.schema = access_url.schema
                sandbox_url.host   = access_url.host

            sandbox = rs_fs.Directory (sandbox_url, session=session)

            # Try to fetch a tarball of files, so that we can get them
            # all in one (SAGA) go!
            tarball_name  = '%s.%s.tbz'   % (pid, ext)
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

                tarball_remote = False
                if sandbox.is_file(tarball_name) and \
                        sandbox.get_size(tarball_name):
                    tarball_remote = True

                if not tarball_remote:
                    # so lets create a tarball with SAGA JobService
                    js_url = pilot['js_hop']
                    log.debug('js  : %s', js_url)
                    js  = rs.job.Service(js_url, session=session)
                    cmd = "cd %s; find . -name \\*.%s > %s.lst; " \
                          "tar cjf %s -T %s.lst" % (sandbox.url.path, ext, ext,
                              tarball_name, ext)
                    j = js.run_job(cmd)
                    j.wait()

                    log.debug('tar cmd   : %s', cmd)
                    log.debug('tar result: %s\n---\n%s\n---\n%s',
                              j.get_stdout_string(), j.get_stderr_string(),
                              j.exit_code)

                    if j.exit_code:
                        raise RuntimeError('could not create tarball: %s' %
                                j.get_stderr_string())

                # we not have a remote tarball and can fetch it
                log.info("fetch '%s%s' to '%s'.", sandbox_url,
                         tarball_name, tgt_url)

                rs_file = rs_fs.File("%s%s" % (sandbox_url, tarball_name),
                                     session=session)
                rs_file.copy(tarball_tgt, flags=rs_fs.CREATE_PARENTS)
                rs_file.close()

            # we now have a local tarball - unpack it
            # note that we do not check if it was unpacked before - it's simpler
            # (and possibly cheaper) to just do that again
            log.info('Extract tarball %s', tarball_tgt)
            tarball = tarfile.open(tarball_tgt, mode='r:bz2')
            tarball.extractall("%s/%s" % (tgt_url.path, pid))

            files = glob.glob("%s/%s/**.%s" % (tgt_url.path, pid, ext))
            ret.extend(files)

            rep.ok("+ %s (%s)\n" % (pid, name))

        except Exception:
            # do not raise, we still try the other pilots
            rep.error("- %s (%s)\n" % (pid, name))
            log.exception('failed to fetch %s for %s', pid, name)

    return ret


# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, dburl=None, src=None, tgt=None, access=None,
        session=None, skip_existing=False, fetch_client=False, log=None):

    return fetch_filetype('prof', 'profiles', sid, dburl, src, tgt, access,
            session, skip_existing, fetch_client, log)


# ------------------------------------------------------------------------------
#
def fetch_logfiles (sid, dburl=None, src=None, tgt=None, access=None,
        session=None, skip_existing=False, fetch_client=False, log=None):

    return fetch_filetype('log', 'logfiles', sid, dburl, src, tgt, access,
            session, skip_existing, fetch_client, log)


# ------------------------------------------------------------------------------

