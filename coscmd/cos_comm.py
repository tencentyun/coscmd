# -*- coding: utf-8 -*-
from six import text_type, binary_type
from hashlib import md5


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
