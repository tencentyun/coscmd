# -*- coding: utf-8 -*-
from xml.dom import minidom
from hashlib import md5
import time
import sys
import os
import datetime
import pytz
from six import text_type, binary_type

maplist = {
    'x-cos-copy-source-If-Modified-Since': 'CopySourceIfModifiedSince',
    'Content-Length': 'ContentLength',
    'x-cos-server-side-encryption-cos-kms-key-id': 'SSEKMSKeyId',
    'x-cos-server-side-encryption-customer-algorithm': 'SSECustomerAlgorithm',
    'If-Unmodified-Since': 'IfUnmodifiedSince',
    'response-content-language': 'ResponseContentLanguage',
    'Metadata': 'Metadata',
    'x-cos-grant-read': 'GrantRead',
    'x-cos-copy-source-If-None-Match': 'CopySourceIfNoneMatch',
    'Content-Language': 'ContentLanguage',
    'x-cos-server-side-encryption': 'ServerSideEncryption',
    'response-expires': 'ResponseExpires',
    'Expires': 'Expires',
    'Content-MD5': 'ContentMD5',
    'response-content-disposition': 'ResponseContentDisposition',
    'Referer': 'Referer',
    'x-cos-grant-full-control': 'GrantFullControl',
    'response-content-encoding': 'ResponseContentEncoding',
    'Content-Disposition': 'ContentDisposition',
    'If-Modified-Since': 'IfModifiedSince',
    'versionId': 'VersionId',
    'response-content-type': 'ResponseContentType',
    'Range': 'Range',
    'x-cos-server-side-encryption-customer-key-MD5': 'SSECustomerKeyMD5',
    'x-cos-acl': 'ACL',
    'x-cos-copy-source-If-Match': 'CopySourceIfMatch',
    'Content-Encoding': 'ContentEncoding',
    'x-cos-copy-source-If-Unmodified-Since': 'CopySourceIfUnmodifiedSince',
    'response-cache-control': 'ResponseCacheControl',
    'x-cos-server-side-encryption-customer-key': 'SSECustomerKey',
    'x-cos-grant-write': 'GrantWrite',
    'If-Match': 'IfMatch',
    'x-cos-storage-class': 'StorageClass',
    'Cache-Control': 'CacheControl',
    'If-None-Match': 'IfNoneMatch',
    'Content-Type': 'ContentType',
    'Pic-Operations': 'PicOperations',
    'x-cos-traffic-limit': 'TrafficLimit',
}

# Python 2/3 兼容性定义
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    str = unicode

def to_bytes(s):
    """将字符串转为bytes"""
    if isinstance(s, binary_type):
        return s
    if isinstance(s, text_type):
        return s.encode('utf-8')
    return s

def to_unicode(s):
    """将字符串转为unicode"""
    if isinstance(s, text_type):
        return s
    if isinstance(s, binary_type):
        return s.decode('utf-8')
    return text_type(s)

def to_str(s):
    """将字符串转为str"""
    if sys.version_info[0] >= 3:
        if isinstance(s, binary_type):
            return s.decode('utf-8')
        return s
    else:
        if isinstance(s, text_type):
            return s.encode('utf-8')
        return str(s)

def mapped(headers):
    """coscmd到pythonsdk参数的一个映射"""
    _headers = dict()
    _meta = dict()
    for i in headers:
        if i in maplist:
            _headers[maplist[i]] = headers[i]
        else:
            _meta[i] = headers[i]
    if len(_meta) > 0:
        _headers['Metadata'] = _meta
    return _headers

def get_file_md5(local_path):
    """获取文件md5"""
    md5_value = md5()
    with open(local_path, "rb") as f:
        while True:
            data = f.read(2048)
            if not data:
                break
            md5_value.update(data)
    return md5_value.hexdigest()

def gen_local_file(filename, filesize):
    with open(filename, 'wb') as f:
        f.write(os.urandom(filesize * 1024 * 1024))
    return 0

def to_printable_str(s):
    if isinstance(s, str):
        return s.encode('utf-8')
    else:
        return s

def getTagText(root, tag):
    node = root.getElementsByTagName(tag)[0]
    rc = ""
    for node in node.childNodes:
        if node.nodeType in (node.TEXT_NODE, node.CDATA_SECTION_NODE):
            rc = rc + node.data

def get_md5_filename(local_path, cos_path):
    ori_file = os.path.abspath(os.path.dirname(
        local_path)) + "!!!" + str(os.path.getsize(local_path)) + "!!!" + cos_path
    m = md5()
    m.update(to_bytes(ori_file))
    return os.path.expanduser('~/.tmp/' + m.hexdigest())

def query_yes_no(question, default="no"):
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        try:
            sys.stdout.write(question + prompt)
            sys.stdout.flush()
            if PY2:
                choice = raw_input().lower()
            else:
                choice = input().lower()
            if default is not None and choice == '':
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' "
                                "(or 'y' or 'n').\n")
        except (KeyboardInterrupt, EOFError):
            sys.stdout.write('\n')
            return False

def response_info(rt):
    request_id = "null"
    code = rt.status_code
    try:
        root = minidom.parseString(rt.content).documentElement
        message = root.getElementsByTagName("Message")[0].childNodes[0].data
        request_id = root.getElementsByTagName(
            "RequestId")[0].childNodes[0].data
    except Exception:
        message = u"Not Found"

    try:
        if request_id == "null":
            request_id = rt.headers['x-cos-request-id']
    except Exception:
        pass
    return (u'''Error: [code {code}] {message}
RequestId: {request_id}'''.format(
        code=code,
        message=message,
        request_id=to_printable_str(request_id)))

def utc_to_local(utc_time_str, utc_format='%Y-%m-%dT%H:%M:%S.%fZ'):
    local_tz = pytz.timezone('Asia/Chongqing')
    local_format = "%Y-%m-%d %H:%M:%S"
    utc_dt = datetime.datetime.strptime(utc_time_str, utc_format)
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    time_str = local_dt.strftime(local_format)
    return int(time.mktime(time.strptime(time_str, local_format)))

def change_to_human(_size):
    s = int(_size)
    res = ""
    if s > 1024 * 1024 * 1024:
        res = str(round(1.0 * s / (1024 * 1024 * 1024), 1)) + "G"
    elif s > 1024 * 1024:
        res = str(round(1.0 * s / (1024 * 1024), 1)) + "M"
    elif s > 1024:
        res = str(round(1.0 * s / (1024), 1)) + "K"
    else:
        res = str(s)
    return res
