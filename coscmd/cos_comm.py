# -*- coding: utf-8 -*-
from six import text_type, binary_type
from hashlib import md5

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
            'Content-Type': 'ContentType'
        }


def mapped(headers):
    """coscmd到pythonsdk参数的一个映射"""
    _headers = dict()
    _meta = dict()
    for i in headers:
        if i in maplist:
            _headers[maplist[i]] = headers[i]
        elif i.startswith('x-cos-meta-'):
            _meta[i] = headers[i]
        else:
            raise Exception('No Parameter Named ' + i + ' Please Check It')
    _headers['Metadata'] = _meta
    return _headers


def to_bytes(s):
    """将字符串转为bytes"""
    if isinstance(s, text_type):
        try:
            return s.encode('utf-8')
        except UnicodeEncodeError as e:
            raise Exception('your unicode strings can not encoded in utf8, utf8 support only!')
    return s


def to_unicode(s):
    """将字符串转为unicode"""
    if isinstance(s, binary_type):
        try:
            return s.decode('utf-8')
        except UnicodeDecodeError as e:
            raise Exception('your bytes strings can not be decoded in utf8, utf8 support only!')
    return s


def get_file_md5(local_path):
    md5_value = md5()
    with open(local_path, "rb") as f:
        while True:
            data = f.read(2048)
            if not data:
                break
            md5_value.update(data)
    return md5_value.hexdigest()
