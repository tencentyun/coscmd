Coscmd Upgrade Guide
====================

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


