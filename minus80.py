#!/usr/bin/env python3
"""
This is -80 (minus80), a tool for long-term archival backup to Amazon S3 and Glacier.

If you're reading this on Amazon, this file both describes the data format and
contains code for restoring the backup to your local computer.  Keep reading.


Dependencies
============
Minus80 was developed and tested with Boto3 1.9.23 and Python 3.6.
Hopefully backwards-compatible versions will still be available
by the time you want to restore from this backup...


Getting Started
===============
Sign up for Amazon Web Services (AWS) at http://aws.amazon.com/

Create a configuration file in JSON format that looks like this:

    {
        "credentials": {
            "profile_name": "XXX",
            "aws_access_key_id": "XXX",
            "aws_secret_access_key": "XXX"
        },
        "aws_s3_bucket": "XXX",
        "restore_for_days": 30,
        "file_database": "~/.minus80.sqlite3"
    }

Replace the "XXX" with appropriate values from your new AWS acount.
You can supply a profile, or access key and secret key, or none of the above
(in which case defaults will be used, based on boto3's search process).
Your bucket name must be globally unique, so pick something no one else
is likely to have used.


Usage - Backup
==============
Storage costs can be reduced by setting up a bucket lifecycle rule that transfers
data from S3 storage to Glacier storage.  Since most of the bytes are in the data/
folder, I would suggest only archiving that folder, say, 7 days after upload.
See http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html

A CloudFormation template is provided to create a bucket with a suitable policy.
The easiest way to use it is through the CloudFormation web console.


Usage - Restore
===============
Restore proceeds in three stages: 'thaw', 'download', and 'rebuild'.

1.  Thaw.  The thaw command traverses the entire S3 bucket.  Any object
    that has been moved to Glacier is restored to S3, for a period of
    `restore_for_days` days.  According to Glacier documentation,
    each thaw command will take up to 12 hours to complete;  after that, files
    can be downloaded from S3 normally.

2.  Download.  The download command will pull down a perfect copy of all
    the data in your S3 bucket.  If any of it has been transferred to
    Glacier, you should run 'thaw' first and wait 12 hours after it finishes.
    Existing files of the right size will be skipped, allowing you to
    resume an interrupted download.

3.  Rebuild.  The rebuild command will re-organize the downloaded data into
    a reasonable approximation of the structure you archived originally.
    Existing files of the right size will be skipped, so you'll usually
    want to target the restore to an empty directory.


Deleting Things
===============
Minus80 is designed to back up things that you want to keep, well, forever.
But sometimes forever is too long.  Here are some options:

- If you're moving to a different backup solution and no longer want this
  one, you can delete the whole S3 bucket through the AWS control panel.

- If you want to get rid of very old stuff, you can use an Amazon
  lifecycle rule to e.g. delete everything over 10 years old.

- If you want to get rid of an individual file that should have never
  been committed to backup, you need to know its hash.  If you still have
  the file, you can calculate its hash with the widely available `shasum`
  command line tool.  If you have the file name and the SQLite database,
  you can look up the hash.  If you have neither, you'll have to download
  all of index/ and search through it to find the file you want.  With the
  hash in hand, you can then remove data/DATA_HASH using the S3 web control
  panel.  To remove all trace of the file, remove index/DATA_HASH as well.


Data Format
===========
All data is stored in an Amazon S3 bucket, using a simple content-addressable
storage scheme.  The contents of each file are stored in

    data/DATA_HASH

where DATA_HASH is the hexadecimal SHA-1 hash of the file contents.  This provides
automatic de-duplication -- if you have multiple copies of a single file, it is
only uploaded and stored once.

Metadata about each file is stored in JSON format in

    index/DATA_HASH/INFO_HASH.json

where INFO_HASH is the hexadecimal SHA-1 of the contents of the metadata JSON file.
The metadata includes a timestamp, so even if the contents of a particular file
change over time, one can recover previous versions from the backup if needed.
All the metadata for various instances of a particular piece of content are stored
under a common prefix (DATA_HASH) to make it feasible to purge all aliases to a
particular bit of content from the archive if e.g. it was included by mistake.

The metadata is also stored in

    stream/TIMESTAMP_INFO_HASH.json

where TIMESTAMP is an ISO8601 basic datetime in UTC, like 2015-07-23T16:17:00Z.
This enables file sync across multiple computers, assuming your clock is correct,
because temporal order matches Amazon's traversal order (lexicographic).

Once files have been backed up, this is recorded in a local SQLite database.
This is purely for efficiency -- if a file's size and mtime have not changed
since the last backup, it is assumed to already be in the archive, and is skipped.
This saves recalculating hashes for thousands of files.  However, if the local
database is deleted, the next backup attempt will verify that each file is in S3.

For convenience, the S3 bucket also contains a copy of the current version of
minus80 (this file), as README.txt, and a file LAST_UPDATE.txt containing the
time of the last backup run as YYYY-MM-DD HH:MM:SS (UTC time zone).


Philosophy
==========
Minus80 is a tool in the Unix tradition.  This means it tries to be simple and modular,
and not re-invent functionality that's available elsewhere. Thus:

- The only way to specify files is as a list of names, one per line, on stdin.
  Expected usage is to pipe the output of `find` to minus80,
  but you could also have a manually curated backup list.
  It does not currently support null-separated output (-print0), because
  I assume you are a sane human and do not have newlines in your filenames.

- Minus80 does not have built-in bandwidth limits, because tools like
  `trickle` (http://monkey.org/~marius/pages/?page=trickle)
  and `throttled` (https://github.com/zquestz/throttled) are available.

- Minus80 does not encourage encryption, because it is designed for disaster recovery
  of files that are very important to you but of low importance to others, such as
  your personal photo library.  The risk of forgetting the encryption password
  is higher than the risk of the data being compromised in a damaging way.
  Of course, I can't stop you from using `gpg` to encrypt files before backing them up.

- The archive format is as simple as possible.  An average coder should be able to
  easily whip up a script to restore the data, given a description of the format.
  Hopefully, s/he could even reverse-engineer the format if necessary.

- Once backed up, data is never deleted.  This is not a backup tool for many small,
  frequently-changing files.  It is not for reconstructing a flawless image of your
  entire disk exactly as it was before the crash.  It is for worst-case-scenario
  recovery of your most precious digital assets if all your other backups have failed.

Minus80 was named for the -80 C freezers that scientists use for long-term cold storage.
Although it talks only to S3, the intention is to use bucket lifecycle rules to move
all data into Glacier for permanent storage.
"""
# MAKE SURE to increment this when code is updated,
# so a new copy gets stored in the archive!
VERSION = '0.4.0'

import argparse, datetime, hashlib, json, logging, os, shutil, sqlite3, sys
import os.path as osp
import boto3
from botocore.exceptions import ClientError

def init_db(dbpath):
    # PARSE_DECLTYPES causes TIMESTAMP columns to be read in as datetime objects.
    db = sqlite3.connect(osp.expanduser(dbpath), detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    with db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                abspath TEXT NOT NULL,
                mtime NUMERIC NOT NULL,
                size INTEGER NOT NULL,
                infohash TEXT NOT NULL,
                datahash TEXT NOT NULL,
                updated TIMESTAMP NOT NULL DEFAULT (DATETIME('now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_files_1 ON files (abspath, mtime, size);
            """)
    return db

def hash_string(s, hashname='sha1'):
    hashfunc = hashlib.new(hashname)
    hashfunc.update(s.encode())
    return hashfunc.hexdigest()

def hash_file_content(absfile, hashname='sha1', chunk=1024*1024):
    hashfunc = hashlib.new(hashname)
    infile = open(absfile, 'rb')
    while True:
        data = infile.read(chunk)
        if not data: break
        hashfunc.update(data)
    return hashfunc.hexdigest()

def key_exists(key):
     try:
         key.load() # Uses HEAD to check if this key exists, or gives 404
         return True
     except ClientError as ex:
         if ex.response['Error']['Code'] == '404':
             return False
         else:
             raise # some other API failure

def upload_string(s3bucket, key_name, s, replace=False):
    """Returns True if data was transferred, False if it was already there."""
    key = s3bucket.Object(key_name)
    if not replace and key_exists(key):
        return False
    key.put(Body=s.encode())
    return True

def upload_file(s3bucket, key_name, filename, replace=False):
    """Returns True if data was transferred, False if it was already there."""
    key = s3bucket.Object(key_name)
    if not replace and key_exists(key):
        return False
    key.upload_file(filename)
    return True

def get_file_info(absfile, datahash):
    return json.dumps({
        'path':absfile,
        'size':osp.getsize(absfile),
        'mtime':osp.getmtime(absfile),
        # We don't want to do this, or the hash and content change every time!
        # mtime should really be enough info to reconstruct the file system state.
        #'stored':time.time(),
        'data':datahash,
    }, separators=(',', ':'), sort_keys=True)

def s3_path_to_local(localroot, path):
    """
    When translating S3 paths to local, insert directory separators
    after the second and fourth characters of the first hash.
    This is a nod to practicality, because most filesystems start to
    perform badly when a directory contains more than 1,000 files.
    """
    path = path.lstrip("/").split("/") # shouldn't be a leading slash, but just in case
    if path[0] in ("data","index") and len(path) >= 2 and len(path[1]) > 4:
        h = path[1]
        path = [path[0], h[0:2], h[2:4], h[4:]] + path[2:]
    return osp.join(localroot, *path)

def do_archive(filename_iter, s3bucket, db):
    # Make sure current documentation exists in bucket.
    # We actually upload this whole file, to provide unambiguous info and a way to restore.
    upload_file(s3bucket, "README_%s.txt" % VERSION, __file__)
    upload_string(s3bucket, "LAST_UPDATE.txt", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), replace=True)
    for relfile in filename_iter:
        try:
            absfile = osp.realpath(relfile) # eliminates symbolic links and does abspath()
            if osp.isdir(absfile):
                logger.info("SKIP_DIR %s" % absfile)
                continue
            if not osp.exists(absfile):
                logger.warning("DOES_NOT_EXIST file %s" % absfile)
            # If file of same name, modification date, and size is in database, we can skip it.
            mtime = osp.getmtime(absfile)
            fsize = osp.getsize(absfile)
            known_file = db.execute("SELECT updated FROM files WHERE abspath = ? AND mtime = ? AND size = ?", (absfile, mtime, fsize)).fetchone() # or None
            if known_file is not None:
                logger.info("SKIP_KNOWN %s %s" % (known_file[0].strftime("%Y-%m-%d %H:%M:%S"), absfile))
                continue
            # Can't skip it.  Calculate the hashes!
            datahash = hash_file_content(absfile)
            fileinfo = get_file_info(absfile, datahash)
            infohash = hash_string(fileinfo)
            indexkey = "index/%s/%s.json" % (datahash, infohash)
            datakey = "data/%s" % datahash
            # Upload the actual file contents, if needed.
            logger.debug("UPLOAD_READY %s %s" % (datakey, absfile))
            did_upload = upload_file(s3bucket, datakey, absfile)
            if did_upload:
                # If file has been modified while we were hashing, abort.  We'll get it next time through.
                if mtime != osp.getmtime(absfile) or fsize != osp.getsize(absfile):
                    logger.warning("CONCURENT_MODIFICATION %s" % absfile)
                    # We uploaded on this pass, so content is likely wrong.  Remove it.
                    s3bucket.Object(datakey).delete()
                    continue
                else:
                    logger.info("UPLOAD_DONE %s %s" % (datakey, absfile))
            else:
                logger.info("UPLOAD_EXISTS %s %s" % (datakey, absfile))
            # Upload the metadata, if needed.
            logger.debug("INDEX_READY %s %s" % (indexkey, absfile))
            if upload_string(s3bucket, indexkey, fileinfo, replace=did_upload):
                logger.info("INDEX_DONE %s %s" % (indexkey, absfile))
                # Post timestamped metadata to allow syncing with the archive.
                now = datetime.datetime.utcnow().replace(microsecond=0).isoformat()
                streamkey = "stream/%sZ_%s.json" % (now, infohash)
                logger.debug("STREAM_READY %s %s" % (streamkey, absfile))
                upload_string(s3bucket, streamkey, fileinfo)
                logger.info("STREAM_DONE %s %s" % (streamkey, absfile))
            else:
                logger.info("INDEX_EXISTS %s %s" % (indexkey, absfile))
            # Note to self: we've now backed this file up.
            with db:
                db.execute("INSERT INTO files (abspath, mtime, size, infohash, datahash) VALUES (?, ?, ?, ?, ?)",
                    (absfile, mtime, fsize, infohash, datahash))
        except Exception as ex:
            logger.exception("ERROR %s %s" % (relfile, ex))

def is_thawed(key):
    return key.restore and 'ongoing-request="false"' in key.restore and 'expiry-date=' in key.restore

def is_thawing(key):
    return key.restore and 'ongoing-request="true"' in key.restore

def do_thaw(s3bucket, for_days):
    thawing_objs = 0
    for key in s3bucket.objects.filter(Prefix="data/"):
        if key.storage_class not in ('GLACIER', 'DEEP_ARCHIVE'):
            logger.debug("NOT_FROZEN %s" % key.key)
            continue
        key = key.Object() # get a full Object, instead of an ObjectSummary
        if is_thawed(key):
            logger.debug("THAW_DONE %s" % key.key)
            continue
        thawing_objs += 1
        if is_thawing(key):
            logger.debug("THAW_IN_PROGRESS %s" % key.key)
            continue
        logger.debug("READY_THAW %s" % key.key)
        key.restore_object(RestoreRequest={
            'Days': for_days,
            'GlacierJobParameters': {
                'Tier': 'Bulk' # 'Standard'|'Bulk'|'Expedited'
            }})
        logger.info("THAW_STARTED %s" % key.key)
    if thawing_objs:
        logger.warning("Thawing %i objects; should be complete by %s" % (thawing_objs, datetime.datetime.now() + datetime.timedelta(hours=12)))
    else:
        logger.warning("All objects thawed; ready to start download")

def do_download(s3bucket, destdir):
    for prefix in ["index/", "data/"]:
        for key in s3bucket.objects.filter(Prefix=prefix):
            try:
                localname = s3_path_to_local(destdir, key.key)
                if osp.lexists(localname) and osp.getsize(localname) == key.size:
                    logger.debug("EXISTS %s" % localname)
                    continue
                localdir = osp.dirname(localname)
                if not osp.isdir(localdir): os.makedirs(localdir)
                logger.debug("DOWNLOAD_READY %s %s" % (key.key, localname))
                key.Object().download_file(localname)
                logger.info("DOWNLOAD_DONE %s %s" % (key.key, localname))
            except Exception as ex:
                logger.exception("ERROR %s %s" % (key.key, ex))

def do_rebuild(srcdir, destdir):
    # Build list of files to restore
    indexes = []
    for dirpath, dirnames, filenames in os.walk(osp.join(srcdir, "index")):
        for indexfile in filenames:
            indexes.append(json.load(open(osp.join(dirpath, indexfile))))
    # Put newest files first
    indexes.sort(key=lambda x:x['mtime'], reverse=True)
    # Iterate over files and copy data into place
    for index in indexes:
        s3name = "data/%s" % index['data']
        srcname = s3_path_to_local(srcdir, s3name)
        destname = osp.join(destdir, index['path'].lstrip(os.sep))
        destbase = osp.dirname(destname)
        if not osp.isdir(destbase): os.makedirs(destbase)
        # Because we only copy if not exists, info from newer indexes takes precedence
        if osp.lexists(destname) and osp.getsize(srcname) == osp.getsize(destname):
            logger.debug("SKIP_EXISTS %s %s" % (srcname, destname))
        else:
            logger.info("COPY %s %s" % (srcname, destname))
            try:
                shutil.copy2(srcname, destname)
                os.utime(destname, (index['mtime'], index['mtime']))
            except Exception as ex:
                logger.exception("ERROR %s %s %s" % (srcname, destname, ex))

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', default=0, action='count')
    subparsers = parser.add_subparsers(dest='cmd_name', help='command to run')
    p_archive = subparsers.add_parser('archive', help='store files listed on stdin to S3/Glacier')
    p_archive.add_argument("config", metavar="CONFIG.json", type=argparse.FileType('r'))
    p_thaw = subparsers.add_parser('thaw', help='restore files from Glacier to normal S3')
    p_thaw.add_argument("config", metavar="CONFIG.json", type=argparse.FileType('r'))
    p_download = subparsers.add_parser('download', help='pull down all files from normal S3')
    p_download.add_argument("config", metavar="CONFIG.json", type=argparse.FileType('r'))
    p_download.add_argument("download_dir", metavar="DOWNLOAD_DIR")
    p_rebuild = subparsers.add_parser('rebuild', help='convert hashed names back to original structure')
    p_rebuild.add_argument("download_dir", metavar="DOWNLOAD_DIR")
    p_rebuild.add_argument("rebuild_dir", metavar="REBUILD_DIR")
    args = parser.parse_args(argv)

    boto_log_level = [logging.WARNING, logging.INFO, logging.DEBUG][min(1, args.verbose)] # max of INFO
    logging.basicConfig(level=boto_log_level, format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')

    global logger
    logger = logging.getLogger("minus80")
    app_log_level = [logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)]
    logger.setLevel(app_log_level)

    # Shared initialization for subcommands:
    if 'config' in args:
        config = json.load(args.config)
        db = init_db(config['file_database'])

        session = boto3.Session(**config['credentials'])
        s3conn = session.resource('s3')
        s3bucket = s3conn.Bucket(config['aws_s3_bucket']) # Bucket must already exist

    # Run subcommand
    if args.cmd_name == 'archive':
        filename_iter = (line.rstrip("\r\n") for line in sys.stdin)
        do_archive(filename_iter, s3bucket, db)
    elif args.cmd_name == 'thaw':
        do_thaw(s3bucket, config['restore_for_days'])
    elif args.cmd_name == 'download':
        do_download(s3bucket, args.download_dir)
    elif args.cmd_name == 'rebuild':
        do_rebuild(args.download_dir, args.rebuild_dir)
    else:
        parser.print_help()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
