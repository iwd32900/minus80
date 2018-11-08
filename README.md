Minus80
=======

Simple content-addressable backup to Amazon Glacier and S3, in about 200 lines of Python.

Minus80 was designed to make a backup-of-last-resort for my personal photo library, in case the house burned down and took the Time Machine backups with it.  Call it "write once, hope to read never".  It stores files under their SHA-1 hashes, so if you have multiple copies floating around, you only have to upload them once.  Ditto if you decide to move them around or reorganize them sometime after the initial backup. With an S3 "lifecycle rule" to move data to Glacier, storage costs just $4 / TB-mo.

The storage format is super-simple, in case you have to write your own restore script 10 years from now.  It's all documented in the script itself, which also uploads itself to the S3 bucket, so you can find it if the unthinkable happens.  So if you're interested, continue reading in minus80.py ...
