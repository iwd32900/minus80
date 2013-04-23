#!/usr/bin/env python
"""
This is -80 (minus80), a tool for long-term archival backup to Amazon S3 and Glacier.

If you're reading this on Amazon, this file both describes the data format and
contains code for restoring the backup to your local computer.  Keep reading.


Getting Started
===============
Sign up for Amazon Web Services (AWS) at http://aws.amazon.com/
You'll need to sign up for (at a minimum) S3 and Glacier.

Create a configuration file in JSON format that looks like this:

    {
        "aws_access_key": "XXX",
        "aws_secret_key": "XXX",
        "aws_s3_bucket": "XXX",
        "days_to_glacier": 30,
        "restore_for_days": 30,
        "restore_gb_per_hr": 1,
        "file_database": "~/.minus80.sqlite3"
    }

Replace the "XXX" with appropriate values from your new AWS acount.
Your bucket name must be globally unique, so pick something no one else
is likely to have used.
The "days_to_glacier" is optional;  if omitted, Glacier will not be used.


Usage - Backup
==============
Storage costs can be reduced by setting up a bucket lifecycle rule that transfers
data from S3 storage to Glacier storage.  Since most of the bytes are in the data/
folder, I would suggest only archiving that folder, say, 30 days after upload.
See http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html

By default, a Glacier archive rule will be set on the bucket for you if your
config file includes a 'days_to_glacier' entry.  You can always delete or
disable it later from the AWS control panel.


Usage - Restore
===============
Restore proceeds in three stages: 'thaw', 'download', and 'rebuild'.

1.  Thaw.  The thaw command traverses the entire S3 bucket.  Any object
    that has been moved to Glacier is restored to S3, for a period of
    `restore_for_days` days.  According to Glacier documentation,
    each thaw command will take 3-5 hours to complete;  after that, files
    can be downloaded from S3 normally.

    You will be charged based on your peak hourly restore rate.  My reading
    of the rules is that you will charged up to $7.20 multiplied by your
    peak restore rate in gigabytes per hour.  Charges may be less depending
    on how much you have stored with Amazon, but since we're contemplating
    a full restore, probably not much less.  The thaw command will try to
    limit restores to a rate of `restore_gb_per_hr`.  At a setting of 1, it
    will take 100 hours (~4 days) to thaw 100 GB worth of data, and you
    should leave your computer running (not sleeping) that whole time.
    Additional charges will apply for S3 storage during the restore period,
    and bandwidth for downloading.

2.  Download.  The download command will pull down a perfect copy of all
    the data in your S3 bucket.  If any of it has been transferred to
    Glacier, you should run 'thaw' first and wait 5 hours after it finishes.
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
  Expected usage is to pipe the output of `find` to Minus80,
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
VERSION = '0.2.1'

import argparse, datetime, hashlib, json, logging, os, shutil, sqlite3, sys, time
import os.path as osp
import boto

def init_db(dbpath):
    db = sqlite3.connect(osp.expanduser(dbpath))
    with db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                abspath TEXT NOT NULL,
                mtime NUMERIC NOT NULL,
                size INTEGER NOT NULL,
                infohash TEXT NOT NULL,
                datahash TEXT NOT NULL,
                updated INTEGER NOT NULL DEFAULT (STRFTIME('%s','now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_files_1 ON files (abspath, mtime, size);
            """)
    return db

def set_lifecycle(s3bucket, days_to_glacier):
    import boto.s3.lifecycle as lc
    RULE_ID = 'minus80_data_archive_rule'
    try:
        lifecycle = s3bucket.get_lifecycle_config()
    except boto.exception.S3ResponseError, ex:
        lifecycle = lc.Lifecycle()
    for rule in lifecycle:
        if rule.id == RULE_ID:
            logger.debug("HAS_LIFECYCLE not updating existing lifecycle rules on bucket")
            break
    else:
        lifecycle.append(lc.Rule(RULE_ID, 'data/', 'Enabled',
            transition=lc.Transition(days=days_to_glacier, storage_class='GLACIER')))
        s3bucket.configure_lifecycle(lifecycle)
        logger.info("SET_LIFECYCLE setting default transition-to-Glacier rule on bucket")

def hash_string(s, hashname='sha1'):
    hashfunc = hashlib.new(hashname)
    hashfunc.update(s)
    return hashfunc.hexdigest()

def hash_file_content(absfile, hashname='sha1', chunk=1024*1024):
    hashfunc = hashlib.new(hashname)
    infile = open(absfile, 'rb')
    while True:
        data = infile.read(chunk)
        if not data: break
        hashfunc.update(data)
    return hashfunc.hexdigest()

def upload_string(s3bucket, key_name, s, replace=False):
    """Returns True if data was transferred, False if it was already there."""
    # Uses HEAD to check if this key exists, or returns None
    key = s3bucket.get_key(key_name)
    if not replace and key is not None: return False
    key = s3bucket.new_key(key_name)
    key.set_contents_from_string(s, replace=replace)
    return True

def upload_file(s3bucket, key_name, filename, replace=False):
    """Returns True if data was transferred, False if it was already there."""
    # Uses HEAD to check if this key exists, or returns None
    key = s3bucket.get_key(key_name)
    if not replace and key is not None: return False
    key = s3bucket.new_key(key_name)
    key.set_contents_from_filename(filename, replace=replace)
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
                logger.info("SKIP_KNOWN %s %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(known_file[0])), absfile))
                continue
            # Can't skip it.  Calculate the hashes!
            datahash = hash_file_content(absfile)
            fileinfo = get_file_info(absfile, datahash)
            infohash = hash_string(fileinfo)
            indexkey = "index/%s/%s.json" % (datahash, infohash)
            datakey = "data/%s" % datahash
            # Upload the actual file contents, if needed.
            replace_index = False
            logger.debug("UPLOAD_READY %s %s" % (datakey, absfile))
            if upload_file(s3bucket, datakey, absfile):
                logger.info("UPLOAD_DONE %s %s" % (datakey, absfile))
                replace_index = True
            else:
                logger.info("UPLOAD_EXISTS %s %s" % (datakey, absfile))
            # Upload the metadata, if needed.
            logger.debug("INDEX_READY %s %s" % (indexkey, absfile))
            if upload_string(s3bucket, indexkey, fileinfo, replace=replace_index):
                logger.info("INDEX_DONE %s %s" % (indexkey, absfile))
            else:
                logger.info("INDEX_EXISTS %s %s" % (indexkey, absfile))
            # Note to self: we've now backed this file up.
            with db:
                db.execute("INSERT INTO files (abspath, mtime, size, infohash, datahash) VALUES (?, ?, ?, ?, ?)",
                    (absfile, mtime, fsize, infohash, datahash))
        except Exception, ex:
            logger.error("ERROR %s %s" % (relfile, ex))

def do_thaw(s3bucket, for_days, gb_per_hr):
    sec_per_byte = 1. / (gb_per_hr * 1.0e9 / 3600.)
    thawing_objs = 0
    for key in s3bucket.list():
        if key.storage_class != 'GLACIER':
            logger.debug("NOT_FROZEN %s" % key.name)
            continue
        if key.expiry_date:
            logger.debug("THAW_DONE %s" % key.name)
            continue
        thawing_objs += 1
        if key.ongoing_restore:
            logger.debug("THAW_IN_PROGRESS %s" % key.name)
            continue
        logger.debug("READY_THAW %s" % key.name)
        time.sleep(sec_per_byte * key.size)
        key.restore(for_days)
        logger.info("THAW_STARTED %s" % key.name)
    if thawing_objs:
        logger.warning("Thawing %i objects; should be complete by %s" % (thawing_objs, datetime.datetime.now() + datetime.timedelta(hours=5)))
    else:
        logger.warning("All objects thawed; ready to start download")

def do_download(s3bucket, destdir):
    for key in s3bucket.list():
        try:
            localname = s3_path_to_local(destdir, key.name)
            if osp.lexists(localname) and osp.getsize(localname) == key.size:
                logger.debug("EXISTS %s" % localname)
                continue
            localdir = osp.dirname(localname)
            if not osp.isdir(localdir): os.makedirs(localdir)
            logger.debug("DOWNLOAD_READY %s %s" % (key.name, localname))
            key.get_contents_to_filename(localname)
            logger.info("DOWNLOAD_DONE %s %s" % (key.name, localname))
        except Exception, ex:
            logger.error("ERROR %s %s" % (key.name, ex))

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
            shutil.copy2(srcname, destname)
            os.utime(destname, (index['mtime'], index['mtime']))

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

    log_level = [logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)]
    logging.basicConfig(level=log_level, format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
    logging.getLogger("boto").setLevel(logging.INFO)
    global logger
    logger = logging

    # Shared initialization for subcommands:
    if 'config' in args:
        config = json.load(args.config)
        db = init_db(config['file_database'])

        s3conn = boto.connect_s3(config['aws_access_key'], config['aws_secret_key'])
        s3bucket = s3conn.create_bucket(config['aws_s3_bucket']) # just returns Bucket if exists

    # Run subcommand
    if args.cmd_name == 'archive':
        if 'days_to_glacier' in config:
            set_lifecycle(s3bucket, config['days_to_glacier'])
        filename_iter = (line.rstrip("\r\n") for line in sys.stdin)
        do_archive(filename_iter, s3bucket, db)
    elif args.cmd_name == 'thaw':
        do_thaw(s3bucket, config['restore_for_days'], config['restore_gb_per_hr'])
    elif args.cmd_name == 'download':
        do_download(s3bucket, args.download_dir)
    elif args.cmd_name == 'rebuild':
        do_rebuild(args.download_dir, args.rebuild_dir)
    else:
        assert False, "Shouldn't be able to get here!"

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
