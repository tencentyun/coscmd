# -*- coding: utf-8 -*-
import fnmatch
import os
import sys
from six.moves.queue import Queue
from qcloud_cos import CosServiceError
if sys.version > '3':
    from coscmd.cos_comm import *
else:
    from cos_comm import *

logger = logging.getLogger("coscmd")


def is_include_file(path, rules):
    for rule in rules:
        if fnmatch.fnmatch(path, rule) is True:
            return True


def is_ignore_file(path, rules):
    for rule in rules:
        if fnmatch.fnmatch(path, rule) is True:
            return True


def is_sync_skip_file_remote2local(cos_path, local_path, **kwargs):
    """
    校验是否进行下载sync
    """
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


def delete_objects(src, deleteList):
    """
    批量删除cos上的对象
    """
    success_num = 0
    fail_num = 0
    rt = {}
    try:
        if len(deleteList['Object']) > 0:
            rt = src['Client'].delete_objects(Bucket=src['Bucket'],
                                              Delete=deleteList)
        if 'Deleted' in rt:
            success_num += len(rt['Deleted'])
            for file in rt['Deleted']:
                logger.info(u"Delete cos://{bucket}/{file}".format(bucket=src['Bucket'],
                                                                   file=file['Key'],))
        if 'Error' in rt:
            fail_num += len(rt['Error'])
            for file in rt['Error']:
                logger.info(u"Delete cos://{bucket}/{file} fail, code: {code}, msg: {msg}"
                            .format(bucket=src['Bucket'],
                                    file=file['Key'],
                                    code=file['Code'],
                                    msg=file['Message']))
    except Exception as e:
        logger.warn(e)
        return [0, len(deleteList['Object'])]
    return [success_num, fail_num]


def local2remote_sync_delete(src, dst, **kwargs):
    """
    上传sync时携带--delete，删除cos上存在而本地不存在的对象
    """
    success_num = 0
    fail_num = 0
    NextMarker = ""
    IsTruncated = "true"
    rt = {}
    while IsTruncated == "true":
        deleteList = {}
        deleteList['Object'] = []
        for i in range(kwargs['retry']):
            try:
                rt = dst['Client'].list_objects(
                    Bucket=dst['Bucket'],
                    Marker=NextMarker,
                    MaxKeys=1000,
                    Delimiter="",
                    Prefix=dst['Path'],
                )
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _cos_path = to_unicode(_file['Key'])
                        _local_path = src['Path'] + _cos_path[len(dst['Path']):]
                        _local_path = to_unicode(_local_path)
                        if os.path.isfile(_local_path) is False:
                            deleteList['Object'].append({'Key': _cos_path})
                break
            except Exception as e:
                time.sleep(1 << i)
                logger.warn(e)
            if i + 1 == kwargs['retry']:
                return [-1, 0, 0]
        _succ, _fail = delete_objects(dst, deleteList)
        success_num += _succ
        fail_num += _fail
    return [0, success_num, fail_num]


def remote2local_sync_delete(src, dst, **kwargs):
    """
    下载sync时携带--delete，删除本地存在而cos上不存在的对象
    """
    q = Queue()
    q.put([dst['Path'], src['Path']])
    success_num = 0
    fail_num = 0
    # BFS上传文件夹
    try:
        while(not q.empty()):
            [local_path, cos_path] = q.get()
            local_path = to_unicode(local_path)
            cos_path = to_unicode(cos_path)
            if cos_path.endswith('/') is False:
                cos_path += "/"
            if local_path.endswith('/') is False:
                local_path += "/"
            cos_path = cos_path.lstrip('/')
            # 当前目录下的文件列表
            dirlist = os.listdir(local_path)
            for filename in dirlist:
                filepath = os.path.join(local_path, filename)
                if os.path.isdir(filepath):
                    q.put([filepath, cos_path + filename])
                else:
                    try:
                        src['Client'].head_object(Bucket=src['Bucket'],
                                                  Key=cos_path + filename)
                    except CosServiceError as e:
                        if e.get_status_code() == 404:
                            try:
                                os.remove(filepath)
                                logger.info(u"Delete {file}".format(
                                    file=filepath))
                                success_num += 1
                            except Exception:
                                logger.info(u"Delete {file} fail".format(
                                    file=filepath))
                                fail_num += 1
    except Exception as e:
        logger.warn(e)
        return [-1, 0, 0]
    return [0, success_num, fail_num]


def remote2remote_sync_delete(src, dst, **kwargs):
    """
    复制sync时携带--delete，删除cos上存在而本地不存在的对象
    """
    success_num = 0
    fail_num = 0
    NextMarker = ""
    IsTruncated = "true"
    rt = {}
    while IsTruncated == "true":
        deleteList = {}
        deleteList['Object'] = []
        for i in range(kwargs['retry']):
            try:
                rt = dst['Client'].list_objects(
                    Bucket=dst['Bucket'],
                    Marker=NextMarker,
                    MaxKeys=1000,
                    Delimiter="",
                    Prefix=dst['Path'],
                )
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _cos_path = to_unicode(_file['Key'])
                        _source_path = src['Path'] + _cos_path[len(dst['Path']):]
                        _source_path = to_unicode(_source_path)
                        try:
                            src['Client'].head_object(
                                Bucket=src['Bucket'],
                                Key=_source_path,
                            )
                        except CosServiceError as e:
                            if e.get_status_code() == 404:
                                deleteList['Object'].append({'Key': _cos_path})
                break
            except Exception as e:
                time.sleep(1 << i)
                logger.warn(e)
            if i + 1 == kwargs['retry']:
                return [-1, 0, 0]
        _succ, _fail = delete_objects(dst, deleteList)
        success_num += _succ
        fail_num += _fail
    return [0, success_num, fail_num]
