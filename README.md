# Minus80

Simple content-addressable backup to Amazon Glacier and S3, in about 200 lines of Python.

Minus80 was designed to make a backup-of-last-resort for my personal photo library, in case the house burned down and took the Time Machine backups with it.  Call it "write once, hope to read never".  It stores files under their SHA-1 hashes, so if you have multiple copies floating around, you only have to upload them once.  Ditto if you decide to move them around or reorganize them sometime after the initial backup. With an S3 "lifecycle rule" to move data to Glacier, storage costs just $4 / TB-mo.

The storage format is super-simple, in case you have to write your own restore script 10 years from now.  It's all documented in the script itself, which also uploads itself to the S3 bucket, so you can find it if the unthinkable happens.  So if you're interested, continue reading in minus80.py ...

## MacOS protections for Photos

As of Mac OS X 10.14 (Mojave), Apple implemented new access restrictions for
Photos libraries, among other resources.
Mac OS wants you to authorize them interactively, and I can't figure out how
to make that work with the LaunchAgent script.
So instead, I start the process in my `.bash_profile`, whenever I open Terminal.
This works ok for me, because I almost always have Terminal open.
This is how I do it:

```
if pgrep -f photo_backup.sh; then
  true # nothing to do here
else
  (cd ~/code/minus80 && nohup ./photo_backup.sh >> log.stdout 2>> log.stderr &)
fi
```
