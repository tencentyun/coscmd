Coscmd Upgrade Guide
====================
1.8.6.14 to 1.8.6.15
--------------------
- Added `move`

1.8.6.13 to 1.8.6.14
--------------------
- Fix error-log encode when retrying
- Use new connection when retry to upload

1.8.6.12 to 1.8.6.13
--------------------
- Fix download encode bug

1.8.6.11 to 1.8.6.12
--------------------
- Add timeout parameter
- Update retry

1.8.6.10 to 1.8.6.11
--------------------
- Fixed Error log when Using coscmd to upload the same file at the same time
- Added sync delete

1.8.6.10 to 1.8.6.11
--------------------
- Fixed Error log when Using coscmd to upload the same file at the same time
- Added sync delete

1.8.6.9 to 1.8.6.10
--------------------
- Fix multiupload retry

1.8.6.8 to 1.8.6.9
--------------------
- Delete uncomplete file when download failed
- Support probe in windows

1.8.6.7 to 1.8.6.8
--------------------
- Update the way of multipart download to faster downloads 
- Add param of `--skipmd5` in download whne sync remote2local without md5check
- Update sync message info

1.8.6.6 to 1.8.6.7
--------------------
- Fix bug of downloading object with `content-encoding:gzip` header
- Update the error prompt of multipart upload
- `download` interface will use single download when set param `-n 1`

1.8.6.5 to 1.8.6.6
--------------------
- Support uploading files with key values containing ./
- Fix the problem that concurrent operation exceptions do not throw exceptions

1.8.6.4 to 1.8.6.5
--------------------
- Fixing bugs in the complete operation of Chinese files

1.8.6.3 to 1.8.6.4
--------------------
- Optimize multipart upload complete action

1.8.6.2 to 1.8.6.3
--------------------
- Update abort interface relay on cos-python-sdk

1.8.6.1 to 1.8.6.2
--------------------
- Fixed bug of delete object without -f parameter

1.8.5.37 to 1.8.6.1
--------------------
- Add interface of probe
- Update unit test

1.8.5.36 to 1.8.5.37
--------------------
- Update requirement of requests from 2.6 to 2.8 

1.8.5.35 to 1.8.5.36
--------------------
- Fixed bug with invalid breakpoint continuation
- Add the -n parameter to the download command to control the number of fragments downloaded

1.8.5.34 to 1.8.5.35
--------------------
- Fixed a fatal download error that could cause the download file MD5 to be inconsistent with the source file 

1.8.5.33 to 1.8.5.34
--------------------
- Fixed a copy folder bug which has 1000 files limit

1.8.5.32 to 1.8.5.33
--------------------

- Fixed a bug where the copy -r interface could not handle some specially encoded key
- Fixed a bug of copy -s
- Added token parameter for setting x-cos-security-token header
- Support copysource include endpoint instead of cosregion
- Added -H parameter to download parameter


