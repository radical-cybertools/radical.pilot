
import os
import glob
import tarfile

import radical.saga  as rs
import radical.utils as ru

from   .db_utils import get_session_docs

rs.fs = rs.filesystem


# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, dburl=None, src=None, tgt=None, access=None,
        session=None, skip_existing=False, fetch_client=False, log=None):
    '''
    sid: session for which all profiles are fetched
    src: dir to look for client session profiles ($src/$sid/*.prof)
    tgt: dir to store the profile in
         - $tgt/$sid/*.prof,
         - $tgt/$sid/$pid/*.prof)

    returns list of file names
    '''

    if not log and session:
        log = session._log
        rep = session._rep
    elif not log:
        log = ru.Logger('radical.pilot.utils')
        rep = ru.Reporter('radical.pilot.utils')

    ret = list()

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise ValueError('RADICAL_PILOT_DBURL is not set')

    if not src:
        src = os.getcwd()

    if not tgt:
        tgt = os.getcwd()

    if not tgt.startswith('/') and '://' not in tgt:
        tgt = "%s/%s" % (os.getcwd(), tgt)

    # we always create a session dir as real target
    tgt_url = rs.Url("%s/%s/" % (tgt, sid))

    # Turn URLs without schema://host into file://localhost,
    # so that they dont become interpreted as relative.
    if not tgt_url.schema:
        tgt_url.schema = 'file'
    if not tgt_url.host:
        tgt_url.host = 'localhost'

    # first fetch session profile
    if fetch_client:
        client_profiles = glob.glob("%s/%s/*.prof" % (src, sid))
        if not client_profiles:
            raise RuntimeError('no client profiles in %s/%s' % (src, sid))

        for client_profile in client_profiles:

            ftgt = rs.Url('%s/%s' % (tgt_url, os.path.basename(client_profile)))
            ret.append("%s" % ftgt.path)

            if skip_existing and os.path.isfile(ftgt.path) \
                    and os.stat(ftgt.path).st_size > 0:
                pass
            else:
                prof_file = rs.fs.File(client_profile, session=session)
                prof_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
                prof_file.close()

            if not os.path.isfile(client_profile):
                raise RuntimeError('profile %s does not exist' % client_profile)

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
    log.debug("Session: %s", sid)
    log.debug("Number of pilots in session: %d", num_pilots)

    for pilot in pilots:

        try:
            log.debug("processing pilot '%s'", pilot['uid'])

            sandbox_url = rs.Url(pilot['pilot_sandbox'])

            if access:
                # Allow to use a different access schema than used for the the
                # run.  Useful if you ran from the headnode, but would like to
                # retrieve the profiles to your desktop (Hello Titan).
                access_url = rs.Url(access)
                sandbox_url.schema = access_url.schema
                sandbox_url.host   = access_url.host

              # print "Overriding remote sandbox: %s" % sandbox_url

            sandbox = rs.fs.Directory (sandbox_url, session=session)

            # Try to fetch a tarball of profiles, so that we can get them
            # all in one (SAGA) go!
            PROFILES_TARBALL = '%s.prof.tgz' % pilot['uid']
            tarball_available = False
            try:
                if  sandbox.is_file(PROFILES_TARBALL) and \
                    sandbox.get_size(PROFILES_TARBALL):

                    log.info("profiles tarball exists")
                    ftgt = rs.Url('%s/%s' % (tgt_url, PROFILES_TARBALL))

                    if skip_existing and os.path.isfile(ftgt.path) \
                            and os.stat(ftgt.path).st_size > 0:

                        log.info("skip fetching of '%s/%s' to '%s'.",
                                 sandbox_url, PROFILES_TARBALL, tgt_url)
                        tarball_available = True
                    else:

                        log.info("fetch '%s%s' to '%s'.", sandbox_url,
                                 PROFILES_TARBALL, tgt_url)

                        prof_file = rs.fs.File("%s%s" % (sandbox_url,
                                            PROFILES_TARBALL), session=session)
                        prof_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
                        prof_file.close()

                        tarball_available = True
                else:
                    log.warn("profiles tarball doesnt exists!")

            except rs.DoesNotExist:
                log.exception("exception(TODO): profile tarball doesnt exists!")

            try:
                os.mkdir("%s/%s" % (tgt_url.path, pilot['uid']))
            except OSError:
                pass

            # We now have a local tarball
            if tarball_available:
                log.info("Extract tarball %s to '%s'.", ftgt.path, tgt_url.path)
                try:
                    tarball = tarfile.open(ftgt.path, mode='r:gz')
                    tarball.extractall("%s/%s" % (tgt_url.path, pilot['uid']))

                    profiles = glob.glob("%s/%s/*.prof" %
                                         (tgt_url.path, pilot['uid']))
                    ret.extend(profiles)
                    os.unlink(ftgt.path)

                    # If extract succeeded, no need to fetch individual profiles
                    rep.ok("+ %s (profiles)\n" % pilot['uid'])
                    continue

                except Exception as e:
                    log.warn('could not extract tarball %s [%s]', ftgt.path, e)

            # If we dont have a tarball (for whichever reason), fetch individual
            # profiles
            profiles = sandbox.list('*.prof')
            for prof in profiles:

                ftgt = rs.Url('%s/%s/%s' % (tgt_url, pilot['uid'], prof))
                ret.append("%s" % ftgt.path)

                if skip_existing and os.path.isfile(ftgt.path) \
                                 and os.stat(ftgt.path).st_size > 0:
                    pass
                else:
                    prof_file = rs.fs.File("%s%s" % (sandbox_url, prof),
                                           session=session)
                    prof_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
                    prof_file.close()

            rep.ok("+ %s (profiles)\n" % pilot['uid'])

        except Exception:
            rep.error("- %s (profiles)\n" % pilot['uid'])
            log.exception('failed to fetch profiles for %s', pilot['uid'])

    return ret


# ------------------------------------------------------------------------------
#
def fetch_logfiles (sid, dburl=None, src=None, tgt=None, access=None,
        session=None, skip_existing=False, fetch_client=False, log=None):
    '''
    sid: session for which all logfiles are fetched
    src: dir to look for client session logfiles
    tgt: dir to store the logfile in

    returns list of file names
    '''

    if not log and session:
        log = session._log
        rep = session._rep
    elif not log:
        log = ru.Logger('radical.pilot.utils')
        rep = ru.Reporter('radical.pilot.utils')

    ret = list()

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not src:
        src = os.getcwd()

    if not tgt:
        tgt = os.getcwd()

    if not tgt.startswith('/') and '://' not in tgt:
        tgt = "%s/%s" % (os.getcwd(), tgt)

    # we always create a session dir as real target
    tgt_url = rs.Url("%s/%s/" % (tgt, sid))

    # Turn URLs without schema://host into file://localhost,
    # so that they dont become interpreted as relative.
    if not tgt_url.schema:
        tgt_url.schema = 'file'
    if not tgt_url.host:
        tgt_url.host = 'localhost'

    if fetch_client:
        # first fetch session logfile
        client_logfile = "%s/%s.log" % (src, sid)

        ftgt = rs.Url('%s/%s' % (tgt_url, os.path.basename(client_logfile)))
        ret.append("%s" % ftgt.path)

        if skip_existing and os.path.isfile(ftgt.path) \
                and os.stat(ftgt.path).st_size > 0:
            pass
        else:
            log_file = rs.fs.File(client_logfile, session=session)
            log_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
            log_file.close()

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
    log.info("Session: %s", sid)
    log.info("Number of pilots in session: %d", num_pilots)


    for pilot in pilots:

        try:
            sandbox_url = rs.Url(pilot['pilot_sandbox'])

            if access:

                # Allow to use a different access schema than used for the the
                # run.  Useful if you ran from the headnode, but would like to
                # retrieve the logfiles to your desktop (Hello Titan).
                access_url = rs.Url(access)
                sandbox_url.schema = access_url.schema
                sandbox_url.host   = access_url.host

            sandbox  = rs.fs.Directory (sandbox_url, session=session)

            # Try to fetch a tarball of logfiles, so that we can get
            # them all in one (SAGA) go!
            LOGFILES_TARBALL  = '%s.log.tgz' % pilot['uid']
            tarball_available = False
            try:
                if  sandbox.is_file(LOGFILES_TARBALL) and \
                    sandbox.get_size(LOGFILES_TARBALL):

                    log.info("logfiles tarball exists")
                    ftgt = rs.Url('%s/%s' % (tgt_url, LOGFILES_TARBALL))

                    if skip_existing and os.path.isfile(ftgt.path) \
                            and os.stat(ftgt.path).st_size > 0:

                        log.info("Skip fetching of '%s/%s' to '%s'.",
                                 sandbox_url, LOGFILES_TARBALL, tgt_url)
                        tarball_available = True
                    else:

                        log.info("Fetching '%s%s' to '%s'.",
                                sandbox_url, LOGFILES_TARBALL, tgt_url)
                        log_file = rs.fs.File("%s%s" % (sandbox_url,
                                                              LOGFILES_TARBALL),
                                              session=session)
                        log_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
                        log_file.close()

                        tarball_available = True
                else:
                    log.warn("logiles tarball doesnt exists")

            except rs.DoesNotExist:
                log.warn("logfiles tarball doesnt exists")

            try:
                os.mkdir("%s/%s" % (tgt_url.path, pilot['uid']))
            except OSError:
                pass

            # We now have a local tarball
            if tarball_available:
                log.debug("Extract tarball %s to %s", ftgt.path, tgt_url.path)

                try:
                    tarball = tarfile.open(ftgt.path)
                    tarball.extractall("%s/%s" % (tgt_url.path, pilot['uid']))

                    logfiles = glob.glob("%s/%s/*.log" % (tgt_url.path,
                                                          pilot['uid']))
                    log.info("tarball %s extracted to '%s/%s/'.",
                            ftgt.path, tgt_url.path, pilot['uid'])
                    ret.extend(logfiles)
                    os.unlink(ftgt.path)

                except Exception as e:
                    log.warn('could not extract tarball %s [%s]', ftgt.path, e)

                # If extract succeeded, no need to fetch individual logfiles
                rep.ok("+ %s (logfiles)\n" % pilot['uid'])
                continue

            # If we dont have a tarball (for whichever reason),
            # fetch individual logfiles
            logfiles = sandbox.list('*.log')

            for logfile in logfiles:

                ftgt = rs.Url('%s/%s/%s' % (tgt_url, pilot['uid'], logfile))
                ret.append("%s" % ftgt.path)

                if skip_existing and os.path.isfile(ftgt.path) \
                                 and os.stat(ftgt.path).st_size > 0:

                    continue

                log_file = rs.fs.File("%s%s" % (sandbox_url, logfile),
                                                session=session)
                log_file.copy(ftgt, flags=rs.fs.CREATE_PARENTS)
                log_file.close()

            rep.ok("+ %s (logfiles)\n" % pilot['uid'])

        except Exception:
            rep.error("- %s (logfiles)\n" % pilot['uid'])

    return ret


# ------------------------------------------------------------------------------
#
def fetch_json(sid, dburl=None, tgt=None, skip_existing=False, session=None,
        log=None):
    '''
    returns file name
    '''

    if not log and session:
        log = session._log
        rep = session._rep
    elif not log:
        log = ru.Logger('radical.pilot.utils')
        rep = ru.Reporter('radical.pilot.utils')

    if not tgt:
        tgt = '.'

    if tgt.startswith('/'):
        # Assume an absolute path
        dst = os.path.join(tgt, '%s.json' % sid)
    else:
        # Assume a relative path
        dst = os.path.join(os.getcwd(), tgt, '%s.json' % sid)

    try           : os.makedirs(os.path.dirname(tgt))
    except OSError: pass  # dir exists

    if skip_existing       and \
       os.path.isfile(dst) and \
       os.stat(dst).st_size > 0:
        log.info("session already in %s", dst)

    else:
        if not dburl:
            dburl = os.environ.get('RADICAL_PILOT_DBURL')

        if not dburl:
            raise ValueError('RADICAL_PILOT_DBURL is not set')

        mongo, db, _, _, _ = ru.mongodb_connect(dburl)

        json_docs = get_session_docs(db, sid)
        ru.write_json(json_docs, dst)

        log.info("session written to %s", dst)

        mongo.close()

    rep.ok("+ %s (json)\n" % sid)
    return dst


# ------------------------------------------------------------------------------

