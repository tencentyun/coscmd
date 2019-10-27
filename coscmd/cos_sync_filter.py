# -*- coding: utf-8 -*-
import fnmatch
import os
import sys
if sys.version > '3':
    from coscmd.cos_comm import *
else:
    from cos_comm import *


def is_include_file(path, rules):
    for rule in rules:
        if fnmatch.fnmatch(path, rule) is True:
            return True


def is_ignore_file(path, rules):
    for rule in rules:
        if fnmatch.fnmatch(path, rule) is True:
            return True


def is_sync_skip_file_remote2local(cos_path, local_path, **kwargs):
    if os.path.isfile(local_path) is False:
        return False
    if not kwargs['skipmd5'] and "_md5" in kwargs:
        _md5 = get_file_md5(local_path)
        if _md5 != kwargs["_md5"]:
            return False
    if "_size" in kwargs:
        _size = os.path.getsize(local_path)
        if _size != kwargs["_size"]:
            return False
    else:
        return False
    return True
