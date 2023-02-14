# -*- coding=utf-8
from prettytable import PrettyTable
from os import path
from contextlib import closing
from xml.dom import minidom
from six import text_type
from six.moves.queue import Queue
from six.moves.urllib.parse import quote, unquote
from hashlib import md5, sha1
import time
import requests
import logging
import sys
import os
import base64
import datetime
import pytz
import yaml
import fnmatch
import copy
import io
import traceback
from tqdm import tqdm
from logging.handlers import RotatingFileHandler
from wsgiref.handlers import format_date_time
import qcloud_cos

if sys.version > '3':
    from coscmd.cos_global import Version
    from coscmd.cos_auth import CosS3Auth
    from coscmd.cos_threadpool import SimpleThreadPool
    from coscmd.cos_comm import *
    from coscmd.cos_sync import *
else:
    from cos_global import Version
    from cos_auth import CosS3Auth
    from cos_threadpool import SimpleThreadPool
    from cos_comm import *
    from cos_sync import *

logger = logging.getLogger("coscmd")


class CoscmdConfig(object):

    def __init__(self, appid, region, endpoint, bucket, secret_id, secret_key, token=None,
                 part_size=1, max_thread=5, schema='https', anonymous=False, verify='md5', retry=2, timeout=60, silence=False, 
                 multiupload_threshold=100, multidownload_threshold=100,
                 *args, **kwargs):
        self._appid = appid
        self._region = region
        self._endpoint = endpoint
        self._bucket = bucket + "-" + appid
        self._secret_id = secret_id
        self._secret_key = secret_key
        self._token = token
        self._part_size = part_size
        self._max_thread = max_thread
        self._schema = schema
        self._anonymous = anonymous
        self._verify = verify
        self._endpoint = endpoint
        self._retry = retry
        self._timeout = timeout
        self._silence = silence
        self._multiupload_threshold=multiupload_threshold
        self._multidownload_threshold=multidownload_threshold
        self._ua = 'coscmd-v' + Version
        logger.debug("config parameter-> appid: {appid}, region: {region}, endpoint: {endpoint}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
            appid=appid,
            region=region,
            endpoint=endpoint,
            bucket=bucket,
            part_size=part_size,
            max_thread=max_thread))

    def uri(self, path=None):
        if path:
            if self._endpoint is not None:
                url = u"{schema}://{bucket}.{endpoint}/{path}".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    endpoint=self._endpoint,
                    path=to_unicode(path)
                )
            else:
                url = u"{schema}://{bucket}.cos.{region}.myqcloud.com/{path}".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    region=self._region,
                    path=to_unicode(path)
                )
        else:
            if self._endpoint is not None:
                url = u"{schema}://{bucket}.{endpoint}/".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    endpoint=self._endpoint
                )
            else:
                url = u"{schema}://{bucket}.cos.{region}.myqcloud.com/".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    region=self._region
                )
        url = url.replace('./', '.%2F')
        url = url.replace("+", "%2B")
        return url


class Interface(object):

    def __init__(self, conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0
        self._err_tips = ''
        self._retry = conf._retry
        self._ua = conf._ua
        self._timeout = conf._timeout
        self._file_num = 0
        self._folder_num = 0
        self._fail_num = 0
        self._path_md5 = ""
        self._have_uploaded = []
        self._etag = 'ETag'
        self._silence = conf._silence
        self._pbar = ''
        self._phar_updated_size = 0
        self._inner_threadpool = SimpleThreadPool(1)
        self._multiupload_threshold = conf._multiupload_threshold * 1024 * 1024
        self._multidownload_threshold = conf._multidownload_threshold * 1024 * 1024
        self.consumed_bytes = 0
        try:
            if conf._endpoint != "":
                sdk_config = qcloud_cos.CosConfig(Endpoint=conf._endpoint,
                                                  Region=conf._region,
                                                  SecretId=conf._secret_id,
                                                  SecretKey=conf._secret_key,
                                                  Token=conf._token,
                                                  Scheme=conf._schema,
                                                  Anonymous=conf._anonymous,
                                                  UA=self._ua,
                                                  Timeout=self._timeout)
            else:
                sdk_config = qcloud_cos.CosConfig(Region=conf._region,
                                                  SecretId=conf._secret_id,
                                                  SecretKey=conf._secret_key,
                                                  Token=conf._token,
                                                  Scheme=conf._schema,
                                                  Anonymous=conf._anonymous,
                                                  UA=self._ua,
                                                  Timeout=self._timeout)
            self._client = qcloud_cos.CosS3Client(sdk_config, self._retry)
        except Exception as e:
            logger.warn(to_unicode(e))
            raise(e)
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

    def percentage(self, consumed_bytes, total_bytes):
        if total_bytes:
            self._pbar.update(consumed_bytes - self.consumed_bytes)
            self.consumed_bytes = consumed_bytes

    def sign_url(self, cos_path, timeout=10000):
        cos_path = to_printable_str(cos_path)
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        s = requests.Session()
        req = requests.Request('GET',  url)
        prepped = s.prepare_request(req)
        signature = CosS3Auth(self._conf, timeout).__call__(
            prepped).headers['Authorization']
        print(to_printable_str(url + '?sign=' + quote(signature)))
        return True

    def list_part(self, cos_path):
        logger.debug("getting uploaded parts")
        NextMarker = ""
        IsTruncated = "true"
        cos_path = to_printable_str(cos_path)
        try:
            while IsTruncated == "true":
                url = self._conf.uri(path=quote(to_printable_str(cos_path)) + '?uploadId={UploadId}&upload&max-parts=1000&part-number-marker={nextmarker}'.format(
                    UploadId=self._upload_id,
                    nextmarker=NextMarker))
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
                if rt.status_code == 200:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName(
                        "IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName("NextPartNumberMarker")[
                            0].childNodes[0].data
                    logger.debug("list resp, status code: {code}, headers: {headers}, text: {text}".format(
                        code=rt.status_code,
                        headers=rt.headers,
                        text=to_printable_str(rt.text)))
                    contentset = root.getElementsByTagName("Part")
                    for content in contentset:
                        ID = content.getElementsByTagName(
                            "PartNumber")[0].childNodes[0].data
                        self._have_uploaded.append(ID)
                        server_md5 = content.getElementsByTagName(
                            self._etag)[0].childNodes[0].data[1:-1]
                        self._md5.append(
                            {'PartNumber': int(ID), 'ETag': server_md5})
                else:
                    logger.debug(response_info(rt))
                    logger.debug("list parts error")
                    return False
        except Exception:
            logger.debug("list parts error")
            return False
        return True

    def upload_folder(self, local_path, cos_path, _http_headers='{}', **kwargs):

        def upload_file_list(upload_filelist):
            _success_num = 0
            _skip_num = 0
            _fail_num = 0
            self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
            multiupload_filelist = []
            for _path in upload_filelist:
                _local_path = _path[0]
                _cos_path = _path[1]
                try:
                    file_size = os.path.getsize(_local_path)
                    if file_size <= self._conf._part_size * 1024 * 1024 + 1024 or file_size <= self._multiupload_threshold:
                        self._inner_threadpool.add_task(
                            self.single_upload,
                            _local_path,
                            _cos_path,
                            _http_headers,
                            **kwargs)
                    else:
                        multiupload_filelist.append([_local_path, _cos_path])
                except Exception as e:
                    _fail_num += 1
                    logger.warn(to_unicode(e))
            self._inner_threadpool.wait_completion()
            result = self._inner_threadpool.get_result()
            for worker in result['detail']:
                for status in worker[2]:
                    if 0 == status:
                        _success_num += 1
                    elif -2 == status:
                        _skip_num += 1
                    else:
                        _fail_num += 1
            for _local_path, _cos_path in multiupload_filelist:
                rt = self.multipart_upload(
                    _local_path, _cos_path, _http_headers, **kwargs)
                if 0 == rt:
                    _success_num += 1
                elif -2 == rt:
                    _skip_num += 1
                else:
                    _fail_num += 1
            return _success_num, _skip_num, _fail_num

        _success_num = 0
        _skip_num = 0
        _fail_num = 0
        raw_local_path = local_path
        raw_cos_path = cos_path
        if raw_local_path.endswith('/') is False:
            raw_local_path += "/"
        if raw_cos_path.endswith('/') is False:
            raw_cos_path += '/'
        raw_cos_path = raw_cos_path.lstrip('/')
        q = Queue()
        q.put([local_path, cos_path])
        # 上传文件列表
        upload_filelist = []
        # BFS上传文件夹
        try:
            while(not q.empty()):
                [local_path, cos_path] = q.get()
                local_path = to_unicode(local_path)
                cos_path = to_unicode(cos_path)
                if cos_path.endswith('/') is False:
                    cos_path += "/"
                if local_path.endswith('/') is False:
                    local_path += '/'
                cos_path = cos_path.lstrip('/')
                # 当前目录下的文件列表
                dirlist = os.listdir(local_path)
                for filename in dirlist:
                    filepath = os.path.join(local_path, filename)
                    if os.path.isdir(filepath):
                        q.put([filepath, cos_path + filename])
                    else:
                        upload_filelist.append(
                            [filepath,  cos_path + filename])
                        if len(upload_filelist) >= 1000:
                            (_succ, _skip,  _fail) = upload_file_list(
                                upload_filelist)
                            _success_num += _succ
                            _skip_num += _skip
                            _fail_num += _fail
                            upload_filelist = []
            if len(upload_filelist) > 0:
                (_succ, _skip, _fail) = upload_file_list(upload_filelist)
                _success_num += _succ
                _skip_num += _skip
                _fail_num += _fail
        except Exception as e:
            logger.warn(to_unicode(e))
            return -1
        logger.info(u"{success_files} files uploaded, {skip_files} files skipped, {fail_files} files failed"
                    .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        # sync --delete 删除cos上不存在的文件
        if kwargs['sync'] and kwargs['delete']:
            if kwargs['force'] is False and kwargs['yes'] is False:
                if query_yes_no(u"WARN: you are deleting some files in the '{cos_path}' cos_path, please make sure".format(cos_path=raw_cos_path)) is False:
                    return -3
            logger.info(u"Synchronizing delete, please wait.")
            try:
                src = {"Client": self._client, "Path": raw_local_path}
                dst = {"Client": self._client,
                       "Bucket": self._conf._bucket, "Path": raw_cos_path}
                kwargs['retry'] = self._retry
                ret, del_succ, del_fail = local2remote_sync_delete(
                    src, dst, **kwargs)
                if ret != 0:
                    logger.warn("sync delete fail")
                else:
                    logger.info(u"{succ_files} files sync deleted, {fail_files} files sync failed"
                                .format(succ_files=del_succ, fail_files=del_fail))
            except Exception as e:
                logger.warn(to_unicode(e))
        if _fail_num == 0:
            return 0
        else:
            return -1

    def local2remote_sync_check(self, local_path, cos_path, **kwargs):
        is_include = is_include_file(local_path, kwargs['include'])
        is_ignore = is_ignore_file(local_path, kwargs['ignore'])
        if not is_include or is_ignore:
            logger.debug(u"Skip {local_path}".format(
                bucket=self._conf._bucket,
                local_path=local_path))
            return -2
        if kwargs['sync'] is True:
            rt = {}
            try:
                rt = self._client.head_object(
                    Bucket=self._conf._bucket,
                    Key=cos_path
                )
            except Exception:
                return 0
            _size = 0
            _md5 = "-"
            if 'x-cos-meta-md5' in rt:
                _md5 = rt['x-cos-meta-md5']
            if 'Content-Length' in rt:
                _size = int(rt['Content-Length'])
            if kwargs["_size"] == _size:
                if kwargs["skipmd5"] or kwargs["_md5"] == _md5:
                    logger.debug(u"Skip {local_path}   =>   cos://{bucket}/{cos_path}".format(
                        bucket=self._conf._bucket,
                        local_path=local_path,
                        cos_path=cos_path))
                    return -2
        return 0

    def single_upload(self, local_path, cos_path, _http_headers='{}', **kwargs):
        _md5 = ""
        file_size = os.path.getsize(local_path)
        if kwargs['skipmd5'] is False:
            if file_size > 5 * 1024 * 1024 * 1024:
                logger.info(
                    u"MD5 is being calculated, please wait. If you do not need to calculate md5, you can use --skipmd5 to skip")
            _md5 = get_file_md5(local_path)
        ret = self.local2remote_sync_check(
            local_path, cos_path, _md5=_md5, _size=file_size, **kwargs)
        if 0 != ret:
            return ret
        logger.info(u"Upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        try:
            _http_header = yaml.safe_load(_http_headers)
        except Exception as e:
            logger.warn("Http_header parse error.")
            logger.warn(to_unicode(e))
            return -1
        try:
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as File:
                    data = File.read()
        except Exception as e:
            logger.warn(to_unicode(e))
            return 0
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        for j in range(self._retry):
            try:
                if j > 0:
                    logger.info(u"Retry to upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
                        bucket=self._conf._bucket,
                        local_path=local_path,
                        cos_path=cos_path))
                http_header = _http_header
                http_header['x-cos-meta-md5'] = _md5
                rt = self._session.put(url=url,
                                       auth=CosS3Auth(self._conf), data=data, headers=http_header, timeout=self._timeout)
                if rt.status_code == 200:
                    return 0
                else:
                    raise Exception(response_info(rt))
            except Exception as e:
                self._session.close()
                self._session = requests.session()
                logger.warn(to_unicode(e))
                time.sleep(2**j)
        return -1

    def multipart_upload(self, local_path, cos_path, _http_headers='{}', **kwargs):
        _md5 = ""
        file_size = os.path.getsize(local_path)
        if kwargs['skipmd5'] is False:
            if file_size > 5 * 1024 * 1024 * 1024:
                logger.info(
                    u"MD5 is being calculated, please wait. If you do not need to calculate md5, you can use --skipmd5 to skip")
            _md5 = get_file_md5(local_path)

        ret = self.local2remote_sync_check(
            local_path, cos_path, _md5=_md5, _size=file_size, **kwargs)
        if 0 != ret:
            return ret
        logger.info(u"Upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))

        http_headers = _http_headers
        try:
            http_headers = yaml.safe_load(http_headers)
            http_headers['x-cos-meta-md5'] = _md5
            http_headers = mapped(http_headers)
        except Exception as e:
            logger.warn("Http_header parse error.")
            logger.warn(to_unicode(e))
            return -1

        try:
            self._pbar = tqdm(total=file_size, unit='B', ncols=80,
                              disable=self._silence, unit_divisor=1024, unit_scale=True)
            self.consumed_bytes = 0
            self._client.upload_file(Bucket=self._conf._bucket,
                                     Key=cos_path,
                                     LocalFilePath=local_path,
                                     PartSize=self._conf._part_size,
                                     MAXThread=self._conf._max_thread,
                                     progress_callback=self.percentage,
                                     EnableMD5=(not kwargs['skipmd5']),
                                     **http_headers)
        except Exception as e:
            logger.warn("Upload file failed")
            logger.warn(to_unicode(e))
            return -1
        finally:
            self._pbar.close()
        return 0

    def upload_file(self, local_path, cos_path, _http_headers='{}', **kwargs):
        file_size = path.getsize(local_path)
        if file_size <= self._conf._part_size * 1024 * 1024 + 1024 or file_size <= self._multiupload_threshold:
            return self.single_upload(local_path, cos_path, _http_headers, **kwargs)
        else:
            return self.multipart_upload(local_path, cos_path, _http_headers, **kwargs)

    def remote2remote_sync_check(self, copy_source, cos_path, **kwargs):
        ret = self.include_ignore_skip(copy_source['Key'], **kwargs)
        if 0 != ret:
            logger.debug(u"Skip cos://{src_bucket}/{src_path} => cos://{dst_bucket}/{dst_path}".format(
                src_bucket=copy_source['Bucket'],
                src_path=copy_source['Key'],
                dst_bucket=self._conf._bucket,
                dst_path=cos_path))
            return -2
        if kwargs['force'] is False:
            if kwargs['sync'] is True:
                try:
                    src_md5 = ""
                    src_size = -1
                    dst_md5 = "."
                    dst_size = -2
                    rt = self._session.head(
                        url=self._conf._schema + "://" + copy_source['RawPath'], auth=CosS3Auth(self._conf))
                    if 'x-cos-meta-md5' in rt.headers:
                        src_md5 = rt.headers['x-cos-meta-md5']
                    if 'Content-Length' in rt.headers:
                        src_size = rt.headers['Content-Length']
                    url = self._conf.uri(path=quote(
                        to_printable_str(cos_path)))
                    rt = self._session.head(url,  auth=CosS3Auth(self._conf))
                    if 'x-cos-meta-md5' in rt.headers:
                        dst_md5 = rt.headers['x-cos-meta-md5']
                    if 'Content-Length' in rt.headers:
                        dst_size = rt.headers['Content-Length']
                    if dst_md5 == src_md5 or (kwargs['skipmd5'] and dst_size == src_size):
                        logger.debug(u"Skip cos://{src_bucket}/{src_path} => cos://{dst_bucket}/{dst_path}".format(
                            src_bucket=copy_source['Bucket'],
                            src_path=copy_source['Key'],
                            dst_bucket=self._conf._bucket,
                            dst_path=cos_path))
                        return -2
                except Exception as e:
                    logger.warn(to_unicode(e))
                    pass
        return 0

    def copy_folder(self, source_path, cos_path, _http_headers='{}', **kwargs):
        if cos_path.endswith('/') is False:
            cos_path += '/'
        if source_path.endswith('/') is False:
            source_path += '/'
        cos_path = cos_path.lstrip('/')
        cos_path = to_unicode(cos_path)
        source_path = to_unicode(source_path)
        _success_num = 0
        _skip_num = 0
        _fail_num = 0
        self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
        NextMarker = ""
        IsTruncated = "true"
        try:
            if self._conf._endpoint is not None:
                source_tmp_path = source_path.split("/")
                source_tmp_path = source_tmp_path[0].split('.')
                source_bucket = source_tmp_path[0]
                source_endpoint = '.'.join(source_tmp_path[1:])
                sdk_config_source = qcloud_cos.CosConfig(SecretId=self._conf._secret_id,
                                                         SecretKey=self._conf._secret_key,
                                                         Token=self._conf._token,
                                                         Endpoint=source_endpoint,
                                                         Scheme=self._conf._schema,
                                                         Anonymous=self._conf._anonymous)
                self._client_source = qcloud_cos.CosS3Client(sdk_config_source)
            else:
                source_tmp_path = source_path.split("/")
                source_tmp_path = source_tmp_path[0].split('.')
                source_bucket = source_tmp_path[0]
                if len(source_tmp_path) == 5 and source_tmp_path[1] == 'cos':
                    source_region = source_tmp_path[2]
                elif len(source_tmp_path) == 4:
                    source_region = source_tmp_path[1]
                else:
                    raise Exception("Parse Region Error")
                sdk_config_source = qcloud_cos.CosConfig(Region=source_region,
                                                         SecretId=self._conf._secret_id,
                                                         SecretKey=self._conf._secret_key,
                                                         Token=self._conf._token,
                                                         Scheme=self._conf._schema,
                                                         Anonymous=self._conf._anonymous)
                self._client_source = qcloud_cos.CosS3Client(sdk_config_source)
        except Exception as e:
            logger.warn(to_unicode(e))
            logger.warn(u"CopySource is invalid: {copysource}".format(
                copysource=source_path))
            return -1
        source_schema = source_path.split('/')[0] + '/'
        source_path = source_path[len(source_schema):]
        raw_source_path = source_path
        raw_cos_path = cos_path
        if raw_source_path.endswith('/') is False:
            raw_source_path += "/"
        if raw_cos_path.endswith('/') is False:
            raw_cos_path += '/'
        raw_cos_path = raw_cos_path.lstrip('/')
        while IsTruncated == "true":
            for i in range(self._retry):
                try:
                    rt = self._client_source.list_objects(
                        Bucket=source_bucket,
                        Marker=NextMarker,
                        MaxKeys=1000,
                        Delimiter="",
                        Prefix=source_path,
                    )
                    if 'IsTruncated' in rt:
                        IsTruncated = rt['IsTruncated']
                    if 'NextMarker' in rt:
                        NextMarker = rt['NextMarker']
                    if 'Contents' in rt:
                        for _file in rt['Contents']:
                            _path = to_unicode(_file['Key'])
                            _source_path = source_schema + _path
                            if source_path.endswith('/') is False and len(source_path) != 0:
                                _cos_path = cos_path + \
                                    _path[len(source_path) + 1:]
                            else:
                                _cos_path = cos_path + _path[len(source_path):]
                            self._inner_threadpool.add_task(
                                self.copy_file, _source_path, _cos_path, _http_headers, **kwargs)
                    break
                except Exception as e:
                    time.sleep(1 << i)
                    logger.warn(to_unicode(e))
                if i + 1 == self._retry:
                    logger.warn("ListObjects fail")
                    return -1

        self._inner_threadpool.wait_completion()
        result = self._inner_threadpool.get_result()
        for worker in result['detail']:
            for status in worker[2]:
                if 0 == status:
                    _success_num += 1
                elif -2 == status:
                    _skip_num += 1
                else:
                    _fail_num += 1
        if kwargs['move']:
            logger.info(u"{success_files} files moved, {skip_files} files skipped, {fail_files} files failed"
                        .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        else:
            logger.info(u"{success_files} files copied, {skip_files} files skipped, {fail_files} files failed"
                        .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        # sync --delete 删除cos上不存在的文件
        if kwargs['sync'] and kwargs['delete']:
            if kwargs['force'] is False and kwargs['yes'] is False:
                if query_yes_no(u"WARN: you are deleting some files in the '{cos_path}' cos_path, please make sure".format(cos_path=raw_cos_path)) is False:
                    return -3
            logger.info(u"Synchronizing delete, please wait.")
            try:
                src = {"Client": self._client_source,
                       "Bucket": source_bucket, "Path": raw_source_path}
                dst = {"Client": self._client,
                       "Bucket": self._conf._bucket, "Path": raw_cos_path}
                kwargs['retry'] = self._retry
                ret, del_succ, del_fail = remote2remote_sync_delete(
                    src, dst, **kwargs)
                if ret != 0:
                    logger.warn("sync delete fail")
                else:
                    logger.info(u"{succ_files} files sync deleted, {fail_files} files sync failed"
                                .format(succ_files=del_succ, fail_files=del_fail))
            except Exception as e:
                logger.warn(to_unicode(e))
        if _fail_num == 0:
            return 0
        else:
            return -1

    def copy_file(self, source_path, cos_path, _http_headers='{}', **kwargs):
        _directive = kwargs['directive']
        _sync = kwargs['sync']
        _move = kwargs['move']
        copy_source = {}
        try:
            if self._conf._endpoint is not None:
                _source_path = source_path.split("/")
                source_tmp_path = _source_path[0].split('.')
                source_key = '/'.join(_source_path[1:])
                copy_source['Bucket'] = source_tmp_path[0]
                copy_source['Endpoint'] = '.'.join(source_tmp_path[1:])
                copy_source['Key'] = source_key
                copy_source['RawPath'] = source_path
                sdk_config_source = qcloud_cos.CosConfig(SecretId=self._conf._secret_id,
                                                         SecretKey=self._conf._secret_key,
                                                         Token=self._conf._token,
                                                         Endpoint=copy_source['Endpoint'],
                                                         Scheme=self._conf._schema,
                                                         Anonymous=self._conf._anonymous)
                _source_client = qcloud_cos.CosS3Client(sdk_config_source)
            else:
                _source_path = source_path.split("/")
                source_tmp_path = _source_path[0].split('.')
                source_key = '/'.join(_source_path[1:])
                copy_source['Bucket'] = source_tmp_path[0]
                if len(source_tmp_path) == 5 and source_tmp_path[1] == 'cos':
                    copy_source['Region'] = source_tmp_path[2]
                elif len(source_tmp_path) == 4:
                    copy_source['Region'] = source_tmp_path[1]
                else:
                    raise Exception("Parse Region Error")
                copy_source['Key'] = source_key
                copy_source['RawPath'] = copy_source['Bucket'] + ".cos." + \
                    copy_source['Region'] + \
                    ".myqcloud.com/" + copy_source['Key']
                sdk_config_source = qcloud_cos.CosConfig(Region=copy_source['Region'],
                                                         SecretId=self._conf._secret_id,
                                                         SecretKey=self._conf._secret_key,
                                                         Token=self._conf._token,
                                                         Scheme=self._conf._schema,
                                                         Anonymous=self._conf._anonymous)
                _source_client = qcloud_cos.CosS3Client(sdk_config_source)
        except Exception as e:
            logger.warn(to_unicode(e))
            logger.warn(u"CopySource is invalid: {copysource}".format(
                copysource=source_path))
            return -1
        rt = self.remote2remote_sync_check(copy_source, cos_path, **kwargs)
        if 0 != rt:
            return rt

        try:
            _http_header = yaml.safe_load(_http_headers)
            kwargs = mapped(_http_header)
        except Exception as e:
            logger.warn("Http_header parse error.")
            logger.warn(to_unicode(e))
            return -1
        try:
            if _move:
                logger.info(u"Move cos://{src_bucket}/{src_path}   =>   cos://{dst_bucket}/{dst_path}".format(
                    src_bucket=copy_source['Bucket'],
                    src_path=copy_source['Key'],
                    dst_bucket=self._conf._bucket,
                    dst_path=cos_path))
                self._client.copy(Bucket=self._conf._bucket,
                                  Key=cos_path,
                                  CopySource=copy_source,
                                  CopyStatus=_directive,
                                  PartSize=self._conf._part_size,
                                  MAXThread=self._conf._max_thread, **kwargs)
                _source_client.delete_object(Bucket=copy_source['Bucket'],
                                             Key=copy_source['Key'])
            else:
                logger.info(u"Copy cos://{src_bucket}/{src_path}   =>   cos://{dst_bucket}/{dst_path}".format(
                    src_bucket=copy_source['Bucket'],
                    src_path=copy_source['Key'],
                    dst_bucket=self._conf._bucket,
                    dst_path=cos_path))
                rt = self._client.copy(Bucket=self._conf._bucket,
                                       Key=cos_path,
                                       CopySource=copy_source,
                                       CopyStatus=_directive,
                                       PartSize=self._conf._part_size,
                                       MAXThread=self._conf._max_thread, **kwargs)
            return 0
        except Exception as e:
            logger.warn(to_unicode(e))
            return -1

    def delete_folder(self, cos_path, **kwargs):
        if kwargs['force'] is False and kwargs['yes'] is False:
            if query_yes_no(u"WARN: you are deleting the file in the '{cos_path}' cos_path, please make sure".format(cos_path=cos_path)) is False:
                return -3
        _force = kwargs['force']
        _versions = kwargs['versions']
        cos_path = to_unicode(cos_path)
        if cos_path == "/":
            cos_path = ""
        kwargs['force'] = True
        self._have_finished = 0
        self._fail_num = 0
        NextMarker = ""
        IsTruncated = "true"
        if _versions:
            NextMarker = ""
            KeyMarker = ""
            VersionIdMarker = ""
            while IsTruncated == "true":
                deleteList = {}
                deleteList['Object'] = []
                for i in range(self._retry):
                    if VersionIdMarker == "null":
                        VersionIdMarker = ""
                    try:
                        rt = self._client.list_objects_versions(
                            Bucket=self._conf._bucket,
                            KeyMarker=KeyMarker,
                            VersionIdMarker=VersionIdMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextKeyMarker' in rt:
                    KeyMarker = rt['NextKeyMarker']
                if 'NextVersionIdMarker' in rt:
                    VersionIdMarker = rt['NextVersionIdMarker']
                if 'DeleteMarker' in rt:
                    for _file in rt['DeleteMarker']:
                        _versionid = _file['VersionId']
                        _path = _file['Key']
                        deleteList['Object'].append({'Key': _path,
                                                     'VersionId': _versionid})
                if 'Version' in rt:
                    for _file in rt['Version']:
                        _versionid = _file['VersionId']
                        _path = _file['Key']
                        deleteList['Object'].append({'Key': _path,
                                                     'VersionId': _versionid})
                if len(deleteList['Object']) > 0:
                    rt = self._client.delete_objects(Bucket=self._conf._bucket,
                                                     Delete=deleteList)
                if 'Deleted' in rt:
                    self._have_finished += len(rt['Deleted'])
                    self._file_num += len(rt['Deleted'])
                    for file in rt['Deleted']:
                        logger.info(u"Delete {file}, versionId: {versionId}".format(
                            file=file['Key'],
                            versionId=file['VersionId']))
                if 'Error' in rt:
                    self._file_num += len(rt['Error'])
                    for file in rt['Error']:
                        logger.info(u"Delete {file}, versionId: {versionId} fail, code: {code}, msg: {msg}"
                                    .format(file=file['Key'],
                                            versionId=file['VersionId'],
                                            code=file['Code'],
                                            msg=file['Message']))
        else:
            NextMarker = ""
            IsTruncated = "true"
            while IsTruncated == "true":
                deleteList = {}
                deleteList['Object'] = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects(
                            Bucket=self._conf._bucket,
                            Marker=NextMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _path = _file['Key']
                        deleteList['Object'].append({'Key': _path})
                self._file_num += len(deleteList['Object'])
                try:
                    if len(deleteList['Object']) > 0:
                        rt = self._client.delete_objects(Bucket=self._conf._bucket,
                                                         Delete=deleteList)
                    if 'Deleted' in rt:
                        self._have_finished += len(rt['Deleted'])
                        for file in rt['Deleted']:
                            logger.info(
                                u"Delete {file}".format(file=file['Key']))
                    if 'Error' in rt:
                        for file in rt['Error']:
                            logger.info(u"Delete {file} fail, code: {code}, msg: {msg}"
                                        .format(file=file['Key'],
                                                code=file['Code'],
                                                msg=file['Message']))
                except Exception as e:
                    pass
        # delete the remaining files
        if self._file_num == 0:
            logger.info(u"The directory does not exist")
            return -1
        logger.info(u"Delete the remaining files again")
        self.delete_folder_redo(cos_path, **kwargs)
        self._fail_num = self._file_num - self._have_finished
        if not _versions:
            logger.info(u"{files} files successful, {fail_files} files failed"
                        .format(files=self._have_finished, fail_files=self._fail_num))
        if self._file_num == self._have_finished:
            return 0
        else:
            return -1

    def delete_folder_redo(self, cos_path, **kwargs):
        _force = kwargs['force']
        _versions = kwargs['versions']
        cos_path = to_unicode(cos_path)
        if cos_path == "/":
            cos_path = ""
        NextMarker = ""
        IsTruncated = "true"
        if _versions:
            NextMarker = ""
            KeyMarker = ""
            VersionIdMarker = ""
            while IsTruncated == "true":
                deleteList = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects_versions(
                            Bucket=self._conf._bucket,
                            KeyMarker=KeyMarker,
                            VersionIdMarker=VersionIdMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextKeyMarker' in rt:
                    NextMarker = rt['NextKeyMarker']
                if 'NextVersionIdMarker' in rt:
                    VersionIdMarker = rt['NextVersionIdMarker']
                if 'DeleteMarker' in rt:
                    for _file in rt['DeleteMarker']:
                        _versionid = _file['VersionId']
                        _path = _file['Key']
                        deleteList.append({'Key': _path,
                                           'VersionId': _versionid})
                if 'Version' in rt:
                    for _file in rt['Version']:
                        _versionid = _file['VersionId']
                        _path = _file['Key']
                        deleteList.append({'Key': _path,
                                           'VersionId': _versionid})
                if len(deleteList) > 0:
                    for file in deleteList:
                        try:
                            self._client.delete_object(
                                Bucket=self._conf._bucket,
                                Key=file['Key'],
                                VersionId=file['VersionId'])
                            self._have_finished += 1
                            logger.info(u"Delete {file}, versionId: {versionId}".format(
                                file=file['Key'],
                                versionId=file['VersionId']))
                        except Exception:
                            logger.info(u"Delete {file}, versionId: {versionId} fail".format(
                                file=file['Key'],
                                versionId=file['VersionId']))
        else:
            NextMarker = ""
            while IsTruncated == "true":
                deleteList = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects(
                            Bucket=self._conf._bucket,
                            Marker=NextMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _path = _file['Key']
                        deleteList.append({'Key': _path})
                if len(deleteList) > 0:
                    for file in deleteList:
                        try:
                            self._client.delete_object(
                                Bucket=self._conf._bucket,
                                Key=file['Key'])
                            self._have_finished += 1
                            logger.info(u"Delete {file}".format(
                                file=file['Key']))
                        except Exception:
                            logger.info(u"Delete {file} fail".format(
                                file=file['Key']))
        return 0

    def delete_file(self, cos_path, **kwargs):
        if kwargs['force'] is False and kwargs['yes'] is False:
            if query_yes_no(u"WARN: you are deleting the file in the '{cos_path}' cos_path, please make sure".format(cos_path=cos_path)) is False:
                return -3
        _versionId = kwargs["versionId"]
        url = self._conf.uri(path="{path}?versionId={versionId}"
                             .format(path=quote(to_printable_str(cos_path)), versionId=_versionId))
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"init resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 204 or rt.status_code == 200:
                if _versionId == "":
                    logger.info(u"Delete cos://{bucket}/{cos_path}".format(
                        bucket=self._conf._bucket,
                        cos_path=cos_path))
                else:
                    logger.info(u"Delete cos://{bucket}/{cos_path}?versionId={versionId}".format(
                        bucket=self._conf._bucket,
                        cos_path=cos_path,
                        versionId=_versionId))
                return 0
            else:
                logger.warn(response_info(rt))
                return -1
        except Exception as e:
            logger.warn(str(e))
            return -1
        return -1

    def list_multipart_uploads(self, cos_path):
        logger.debug("getting uploaded parts")
        KeyMarker = ""
        UploadIdMarker = ""
        IsTruncated = "true"
        cos_path = to_printable_str(cos_path)
        part_num = 0
        while IsTruncated == "true":
            IsTruncated = 'false'
            try:
                rt = self._client.list_multipart_uploads(
                    Bucket=self._conf._bucket,
                    Prefix=cos_path,
                    Delimiter='',
                    KeyMarker=KeyMarker,
                    UploadIdMarker=UploadIdMarker,
                    MaxUploads=10,
                )
                if "NextKeyMarker" in rt:
                    KeyMarker = rt['NextKeyMarker']
                if "NextUploadIdMarker" in rt:
                    UploadIdMarker = rt['NextUploadIdMarker']
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if "Upload" in rt:
                    for upload in rt['Upload']:
                        part_num += 1
                        if 'Key' in upload:
                            _key = upload['Key']
                        if 'UploadId' in upload:
                            _uploadid = upload['UploadId']
                        logger.info(u"Key:{key}, UploadId:{uploadid}".format(
                            key=to_unicode(_key), uploadid=_uploadid))
            except Exception as e:
                logger.warn(to_unicode(e))
        logger.info(u" Parts num: {file_num}".format(
            file_num=str(part_num)))

    def abort_parts(self, cos_path):
        NextKeyMarker = ""
        NextUploadIdMarker = ""
        IsTruncated = "true"
        _success_num = 0
        _fail_num = 0
        cos_path = to_printable_str(cos_path)
        try:
            while IsTruncated == "true":
                abortList = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_multipart_uploads(
                            Bucket=self._conf._bucket,
                            KeyMarker=NextKeyMarker,
                            UploadIdMarker=NextUploadIdMarker,
                            MaxUploads=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextUploadIdMarker' in rt:
                    NextUploadIdMarker = rt['NextUploadIdMarker']
                if 'NextKeyMarker' in rt:
                    NextKeyMarker = rt['NextKeyMarker']
                if 'Upload' in rt:
                    for _file in rt['Upload']:
                        _path = to_unicode(_file['Key'])
                        _uploadid = _file['UploadId']
                        abortList.append({'Key': _path,
                                          'UploadId': _uploadid})
                if len(abortList) > 0:
                    for file in abortList:
                        try:
                            rt = self._client.abort_multipart_upload(
                                Bucket=self._conf._bucket,
                                Key=file['Key'],
                                UploadId=file['UploadId'])
                            _success_num += 1
                            logger.info(u"Abort Key: {key}, UploadId: {uploadid}".format(
                                key=file['Key'],
                                uploadid=file['UploadId']))
                        except Exception as e:
                            logger.warn(to_unicode(e))
                            logger.info(u"Abort Key: {key}, UploadId: {uploadid} fail".format(
                                key=file['Key'],
                                uploadid=file['UploadId']))
                            _fail_num += 1
            logger.info(u"{files} files successful, {fail_files} files failed"
                        .format(files=_success_num, fail_files=_fail_num))
            if _fail_num == 0:
                return 0
            else:
                return -1
        except Exception as e:
            logger.warn(to_unicode(e))
            return -1

    def list_objects(self, cos_path, **kwargs):
        _recursive = kwargs['recursive']
        _all = kwargs['all']
        _num = kwargs['num']
        _human = kwargs['human']
        _versions = kwargs['versions']
        IsTruncated = "true"
        Delimiter = "/"
        if _recursive is True:
            Delimiter = ""
        if _all is True:
            _num = -1
        self._file_num = 0
        self._total_size = 0
        if _versions:
            KeyMarker = ""
            VersionIdMarker = ""
            while IsTruncated == "true":
                table = PrettyTable(
                    ["Path", "Size/Type", "Time", "VersionId"])
                table.align = "l"
                table.align['Size/Type'] = 'r'
                table.padding_width = 3
                table.header = False
                table.border = False
                for i in range(self._retry):
                    try:
                        if VersionIdMarker == "null":
                            VersionIdMarker = ""
                        rt = self._client.list_objects_versions(
                            Bucket=self._conf._bucket,
                            Delimiter=Delimiter,
                            KeyMarker=KeyMarker,
                            VersionIdMarker=VersionIdMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextKeyMarker' in rt:
                    KeyMarker = rt['NextKeyMarker']
                if 'NextVersionIdMarker' in rt:
                    VersionIdMarker = rt['NextVersionIdMarker']
                if 'CommonPrefixes' in rt:
                    for _folder in rt['CommonPrefixes']:
                        _time = ""
                        _type = "DIR"
                        _path = _folder['Prefix']
                        table.add_row([_path, _type, _time, ""])
                if 'DeleteMarker' in rt:
                    for _file in rt['DeleteMarker']:
                        self._file_num += 1
                        _time = _file['LastModified']
                        _time = time.localtime(utc_to_local(_time))
                        _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                        _versionid = _file['VersionId']
                        _path = _file['Key']
                        table.add_row([_path, "", _time, _versionid])
                        if self._file_num == _num:
                            break
                if 'Version' in rt and (self._file_num < _num or _num == -1):
                    for _file in rt['Version']:
                        self._file_num += 1
                        _time = _file['LastModified']
                        _time = time.localtime(utc_to_local(_time))
                        _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                        _versionid = _file['VersionId']
                        _size = _file['Size']
                        self._total_size += int(_size)
                        if _human is True:
                            _size = change_to_human(_size)
                        _path = _file['Key']
                        table.add_row([_path, _size, _time, _versionid])
                        if self._file_num == _num:
                            break
                try:
                    print(unicode(table))
                except Exception as e:
                    print(table)
                if self._file_num == _num:
                    break

            if _human:
                self._total_size = change_to_human(str(self._total_size))
            else:
                self._total_size = str(self._total_size)
            if _recursive:
                logger.info(u" Files num: {file_num}".format(
                    file_num=str(self._file_num)))
                logger.info(u" Files size: {file_size}".format(
                    file_size=self._total_size))
            if _all is False and self._file_num == _num:
                logger.info(
                    u"Has listed the first {num}, use \'-a\' option to list all please".format(num=self._file_num))
            return 0
        else:
            NextMarker = ""
            while IsTruncated == "true":
                table = PrettyTable(["Path", "Size/Type", "Class", "Time"])
                table.align = "l"
                table.align['Size/Type'] = 'r'
                table.padding_width = 3
                table.header = False
                table.border = False
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects(
                            Bucket=self._conf._bucket,
                            Delimiter=Delimiter,
                            Marker=NextMarker,
                            MaxKeys=1000,
                            Prefix=cos_path
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(to_unicode(e))
                    if i + 1 == self._retry:
                        return -1
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'CommonPrefixes' in rt:
                    for _folder in rt['CommonPrefixes']:
                        _time = ""
                        _type = "DIR"
                        _path = _folder['Prefix']
                        _class = ""
                        table.add_row([_path, _type, _class, _time])
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        self._file_num += 1
                        _time = _file['LastModified']
                        _time = time.localtime(utc_to_local(_time))
                        _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                        _size = _file['Size']
                        _class = _file['StorageClass']
                        self._total_size += int(_size)
                        if _human is True:
                            _size = change_to_human(_size)
                        _path = _file['Key']
                        table.add_row([_path, _size, _class, _time])
                        if self._file_num == _num:
                            break
                try:
                    print(unicode(table))
                except Exception:
                    print(table)
                if self._file_num == _num:
                    break
            if _human:
                self._total_size = change_to_human(str(self._total_size))
            else:
                self._total_size = str(self._total_size)
            if _recursive:
                logger.info(u" Files num: {file_num}".format(
                    file_num=str(self._file_num)))
                logger.info(u" Files size: {file_size}".format(
                    file_size=self._total_size))
            if _all is False and self._file_num == _num:
                logger.info(
                    u"Has listed the first {num}, use \'-a\' option to list all please".format(num=self._file_num))
            return 0

    def info_object(self, cos_path, _human=False):
        table = PrettyTable([cos_path, ""])
        table.align = "l"
        table.padding_width = 3
        table.header = False
        table.border = False
        table.add_row(['Key', cos_path])
        try:
            rt = self._client.head_object(
                Bucket=self._conf._bucket,
                Key=cos_path
            )
            for i in rt:
                table.add_row([i, rt[i]])
            try:
                print(unicode(table))
            except Exception as e:
                print(table)
            return 0
        except Exception as e:
            # head请求没有xml body
            logger.warn(str(e))
        return -1

    def download_folder(self, cos_path, local_path, _http_headers='{}', **kwargs):

        if cos_path.endswith('/') is False:
            cos_path += '/'
        if local_path.endswith('/') is False:
            local_path += '/'
        cos_path = cos_path.lstrip('/')
        NextMarker = ""
        IsTruncated = "true"
        _success_num = 0
        _fail_num = 0
        _skip_num = 0
        raw_local_path = local_path
        raw_cos_path = cos_path
        cos_path = to_unicode(cos_path)

        NextMarker = ""
        IsTruncated = "true"
        while IsTruncated == "true":
            self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
            multidownload_filelist = []
            try:
                rt = self._client.list_objects(
                    Bucket=self._conf._bucket,
                    Marker=NextMarker,
                    MaxKeys=1000,
                    Prefix=cos_path,
                )
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _cos_path = _file['Key']
                        _size = int(_file['Size'])
                        _cos_path = to_unicode(_cos_path)
                        _local_path = local_path + _cos_path[len(cos_path):]
                        _local_path = to_unicode(_local_path)
                        if _cos_path.endswith('/'):
                            continue
                        if _size <= self._multidownload_threshold:
                            self._inner_threadpool.add_task(
                                self.download_file, _cos_path, _local_path, _http_headers, **kwargs)
                        else:
                            multidownload_filelist.append(
                                [_cos_path, _local_path, _size])
            except Exception as e:
                logger.warn(to_unicode(e))
                logger.warn("List objects failed")
                return -1
            self._inner_threadpool.wait_completion()
            result = self._inner_threadpool.get_result()
            for worker in result['detail']:
                for status in worker[2]:
                    if 0 == status:
                        _success_num += 1
                    elif -2 == status:
                        _skip_num += 1
                    else:
                        _fail_num += 1
            for _cos_path, _local_path, _size in multidownload_filelist:
                try:
                    rt = self.download_file(
                        _cos_path, _local_path, _http_headers, _size=_size, **kwargs)
                    if 0 == rt:
                        _success_num += 1
                    elif -2 == rt:
                        _skip_num += 1
                    else:
                        _fail_num += 1
                except Exception as e:
                    print(e)
        logger.info(u"{success_files} files downloaded, {skip_files} files skipped, {fail_files} files failed"
                    .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        # sync --delete 删除本地不存在的文件
        if kwargs['sync'] and kwargs['delete']:
            if kwargs['force'] is False and kwargs['yes'] is False:
                if query_yes_no(u"WARN: you are deleting the file in the '{local_path}' local_path, please make sure".format(local_path=raw_local_path)) is False:
                    return -3
            logger.info(u"Synchronizing delete, please wait.")
            try:
                src = {"Client": self._client,
                       "Bucket": self._conf._bucket, "Path": raw_cos_path}
                dst = {"Client": self._client, "Path": raw_local_path}
                kwargs['retry'] = self._retry
                ret, del_succ, del_fail = remote2local_sync_delete(
                    src, dst, **kwargs)
                if ret != 0:
                    logger.warn("sync delete fail")
                else:
                    logger.info(u"{succ_files} files sync deleted, {fail_files} files sync failed"
                                .format(succ_files=del_succ, fail_files=del_fail))
            except Exception as e:
                logger.warn(to_unicode(e))
        if _fail_num == 0:
            return 0
        else:
            return -1

    def include_ignore_skip(self, cos_path, **kwargs):
        is_include = is_include_file(cos_path, kwargs['include'])
        is_ignore = is_ignore_file(cos_path, kwargs['ignore'])
        if not is_include or is_ignore:
            return -2
        return 0

    def remote2local_sync_check(self, cos_path, local_path, **kwargs):
        ret = self.include_ignore_skip(cos_path, **kwargs)
        if 0 != ret:
            logger.debug(u"Skip cos://{bucket}/{cos_path} => {local_path}".format(
                bucket=self._conf._bucket,
                local_path=local_path,
                cos_path=cos_path))
            return -2
        if kwargs['force'] is False:
            if os.path.isfile(local_path) is True:
                if kwargs['sync'] is True:
                    try:
                        rt = self._client.head_object(
                            Bucket=self._conf._bucket,
                            Key=cos_path
                        )
                    except Exception as e:
                        logger.warn(str(e))
                        return -1
                    _size = 0
                    _md5 = "-"
                    if 'x-cos-meta-md5' in rt:
                        _md5 = rt['x-cos-meta-md5']
                    if 'Content-Length' in rt:
                        _size = int(rt['Content-Length'])
                    kwargs["_size"] = _size
                    kwargs["_md5"] = _md5
                    if is_sync_skip_file_remote2local(cos_path, local_path, **kwargs):
                        logger.debug(u"Skip cos://{bucket}/{cos_path} => {local_path}".format(
                            bucket=self._conf._bucket,
                            local_path=local_path,
                            cos_path=cos_path))
                        return -2
                else:
                    logger.warn(
                        u"The file {file} already exists, please use -f to overwrite the file".format(file=local_path))
                    return -1
        return 0

    def download_file(self, cos_path, local_path, _http_headers='{}', **kwargs):
        if "_size" in kwargs:
            file_size = int(kwargs["_size"])
        else:
            # head操作获取文件大小
            try:
                rt = self._client.head_object(
                    Bucket=self._conf._bucket,
                    Key=cos_path
                )
                file_size = int(rt['Content-Length'])
            except Exception as e:
                logger.warn(str(e))
                return -1
        cos_path = cos_path.lstrip('/')
        rt = self.remote2local_sync_check(cos_path, local_path, **kwargs)
        if 0 != rt:
            return rt
        logger.info(u"Download cos://{bucket}/{cos_path}   =>   {local_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        try:
            _http_headers = yaml.safe_load(_http_headers)
            _http_headers = mapped(_http_headers)
        except Exception as e:
            logger.warn("Http_header parse error.")
            logger.warn(to_unicode(e))
            return -1

        # 如果路径不存在，则创建文件夹
        dir_path = os.path.dirname(local_path)
        if os.path.isdir(dir_path) is False and dir_path != '':
            try:
                os.makedirs(dir_path, 0o755)
            except Exception as e:
                pass
        if file_size <= self._multidownload_threshold:
            try:
                self._client.download_file(
                        Bucket=self._conf._bucket,
                        Key=cos_path,
                        DestFilePath=local_path,
                        PartSize=self._conf._part_size,
                        MAXThread=self._conf._max_thread,
                        EnableCRC=False,
                        **_http_headers
                    )
            except CosServiceError as e:
                logger.info(u"Download cos://{bucket}/{cos_path}   =>   {local_path} failed".format(
                    bucket=self._conf._bucket,
                    local_path=local_path,
                    cos_path=cos_path))
                logger.warn(e.get_error_code())
                return -1
            except Exception as e:
                logger.warn(e)
                return -1
        else:
            try:
                self._pbar = tqdm(total=file_size, unit='B', ncols=80,
                                    disable=self._silence, unit_divisor=1024, unit_scale=True)
                self.consumed_bytes = 0  
                self._client.download_file(
                    Bucket=self._conf._bucket,
                    Key=cos_path,
                    DestFilePath=local_path,
                    PartSize=self._conf._part_size,
                    MAXThread=self._conf._max_thread,
                    EnableCRC=False,
                    progress_callback=self.percentage,
                    **_http_headers
                )
            except CosServiceError as e:
                logger.info(u"Download cos://{bucket}/{cos_path}   =>   {local_path} failed".format(
                    bucket=self._conf._bucket,
                    local_path=local_path,
                    cos_path=cos_path))
                traceback.print_exc(file=sys.stdout)
                logger.warn(e.get_error_code())
                return -1
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                logger.warn(e)
                return -1
            finally:
                self._pbar.close()
        return 0

    def restore_folder(self, cos_path, **kwargs):
        self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
        _success_num = 0
        _progress_num = 0
        _fail_num = 0
        NextMarker = ""
        IsTruncated = "true"
        while IsTruncated == "true":
            restoreList = []
            for i in range(self._retry):
                try:
                    rt = self._client.list_objects(
                        Bucket=self._conf._bucket,
                        Marker=NextMarker,
                        MaxKeys=1000,
                        Prefix=cos_path,
                    )
                    break
                except Exception as e:
                    time.sleep(1 << i)
                    logger.warn(to_unicode(e))
                if i + 1 == self._retry:
                    return -1
            if 'IsTruncated' in rt:
                IsTruncated = rt['IsTruncated']
            if 'NextMarker' in rt:
                NextMarker = rt['NextMarker']
            if 'Contents' in rt:
                for _file in rt['Contents']:
                    _path = _file['Key']
                    self._inner_threadpool.add_task(
                        self.restore_file, _path, **kwargs)

        self._inner_threadpool.wait_completion()
        result = self._inner_threadpool.get_result()
        for worker in result['detail']:
            for status in worker[2]:
                if 0 == status:
                    _success_num += 1
                elif -2 == status:
                    _progress_num += 1
                else:
                    _fail_num += 1
        logger.info(u"{success_files} files successful, {progress_files} files have in progress, {fail_files} files failed"
                    .format(success_files=_success_num, progress_files=_progress_num, fail_files=_fail_num))
        if _fail_num == 0:
            return 0
        else:
            return -1

    def restore_file(self, cos_path, **kwargs):
        _tier = kwargs['tier']
        _day = kwargs['day']
        restore_request = {}
        restore_request['Days'] = _day
        restore_request['CASJobParameters'] = {'Tier': _tier}
        cos_path = to_unicode(cos_path)
        try:
            logger.info(u"Restore cos://{bucket}/{path}".format(
                bucket=self._conf._bucket,
                path=cos_path
            ))
            self._client.restore_object(
                Bucket=self._conf._bucket,
                Key=cos_path,
                RestoreRequest=restore_request)
            return 0
        except CosServiceError as e:
            if e.get_status_code() == 409:
                logger.warn(u"cos://{bucket}/{path} already in pogress".format(
                    bucket=self._conf._bucket,
                    path=cos_path
                ))
                return -2
            logger.warn(e.get_error_code())
            return -1

    def put_object_acl(self, grant_read, grant_write, grant_full_control, cos_path):
        acl = []
        if grant_read is not None:
            for i in grant_read.split(","):
                if len(i) > 0:
                    acl.append([i, "READ"])
        if grant_write is not None:
            for i in grant_write.split(","):
                if len(i) > 0:
                    acl.append([i, "WRITE"])
        if grant_full_control is not None:
            for i in grant_full_control.split(","):
                if len(i) > 0:
                    acl.append([i, "FULL_CONTROL"])
        url = self._conf.uri(quote(to_printable_str(cos_path)) + "?acl")
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            if rt.status_code != 200:
                logger.warn(response_info(rt))
                return False
            root = minidom.parseString(rt.content).documentElement
            owner_id = root.getElementsByTagName("ID")[0].childNodes[0].data
            grants = ''
            subid = ''
            rootid = ''
            for ID, Type in acl:
                if len(ID.split("/")) == 1:
                    accounttype = "RootAccount"
                    rootid = ID.split("/")[0]
                    subid = ID.split("/")[0]
                elif len(ID.split("/")) == 2:
                    accounttype = "SubAccount"
                    rootid = ID.split("/")[0]
                    subid = ID.split("/")[1]
                else:
                    logger.warn("ID format error!")
                    return False
                id = ""
                if subid != "anyone":
                    if subid == rootid:
                        id = rootid
                    else:
                        id = rootid + "/" + subid
                else:
                    id = "qcs::cam::anyone:anyone"
                grants += '''
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="{accounttype}">
                <ID>{id}</ID>
            </Grantee>
            <Permission>{permissiontype}</Permission>
        </Grant>'''.format(id=id, accounttype=accounttype, permissiontype=Type)

            data = '''<AccessControlPolicy>
    <Owner>
        <ID>{id}</ID>
    </Owner>
    <AccessControlList>'''.format(id=owner_id) + grants + '''
    </AccessControlList>
</AccessControlPolicy>
'''

            logger.debug(data)
            rt = self._session.put(
                url=url, auth=CosS3Auth(self._conf), data=data)
            logger.debug(u"put resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def get_object_acl(self, cos_path):
        url = self._conf.uri(quote(to_printable_str(cos_path)) + "?acl")
        table = PrettyTable([cos_path, ""])
        table.align = "l"
        table.padding_width = 3
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"get resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                try:
                    table.add_row(['ACL', ("%s: %s" % (grant.getElementsByTagName("ID")[
                                  0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                except Exception:
                    table.add_row(['ACL', ("%s: %s" % (
                        'anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
            if rt.status_code == 200:
                try:
                    print(unicode(table))
                except Exception as e:
                    print(table)
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def create_bucket(self):
        url = self._conf.uri(path='')
        self._have_finished = 0
        try:
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"put resp, status code: {code}, headers: {headers}, text: {text}".format(
                code=rt.status_code,
                headers=rt.headers,
                text=rt.text))
            if rt.status_code == 200:
                logger.info(
                    u"Create cos://{bucket}".format(bucket=self._conf._bucket))
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return True

    def delete_bucket(self, **kwargs):
        url = self._conf.uri(path='')
        self._have_finished = 0
        _force = kwargs["force"]
        try:
            if _force:
                logger.info("Clearing files and upload parts in the bucket")
                self.abort_parts("")
                kwargs['versions'] = False
                self.delete_folder("", **kwargs)
                kwargs['versions'] = True
                self.delete_folder("", **kwargs)
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"delete resp, status code: {code}, headers: {headers}, text: {text}".format(
                code=rt.status_code,
                headers=rt.headers,
                text=rt.text))
            if rt.status_code == 204:
                logger.info(
                    u"Delete cos://{bucket}".format(bucket=self._conf._bucket))
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return True

    def put_bucket_acl(self, grant_read, grant_write, grant_full_control):
        acl = []
        if grant_read is not None:
            for i in grant_read.split(","):
                if len(i) > 0:
                    acl.append([i, "READ"])
        if grant_write is not None:
            for i in grant_write.split(","):
                if len(i) > 0:
                    acl.append([i, "WRITE"])
        if grant_full_control is not None:
            for i in grant_full_control.split(","):
                if len(i) > 0:
                    acl.append([i, "FULL_CONTROL"])
        url = self._conf.uri("?acl")
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            if rt.status_code != 200:
                logger.warn(response_info(rt))
                return False
            root = minidom.parseString(rt.content).documentElement
            owner_id = root.getElementsByTagName("ID")[0].childNodes[0].data
            grants = ''
            subid = ''
            rootid = ''
            for ID, Type in acl:
                if len(ID.split("/")) == 1:
                    accounttype = "RootAccount"
                    rootid = ID.split("/")[0]
                    subid = ID.split("/")[0]
                elif len(ID.split("/")) == 2:
                    accounttype = "SubAccount"
                    rootid = ID.split("/")[0]
                    subid = ID.split("/")[1]
                else:
                    logger.warn(u"ID format error!")
                    return False
                id = ""
                if subid != "anyone":
                    if subid == rootid:
                        id = rootid
                    else:
                        id = rootid + "/" + subid
                else:
                    id = "qcs::cam::anyone:anyone"
                grants += '''
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="{accounttype}">
                <ID>{id}</ID>
            </Grantee>
            <Permission>{permissiontype}</Permission>
        </Grant>'''.format(id=id, accounttype=accounttype, permissiontype=Type)

            data = '''<AccessControlPolicy>
    <Owner>
        <ID>{id}</ID>
    </Owner>
    <AccessControlList>'''.format(id=owner_id) + grants + '''
    </AccessControlList>
</AccessControlPolicy>
'''

            logger.debug(data)
            rt = self._session.put(
                url=url, auth=CosS3Auth(self._conf), data=data)
            logger.debug(u"put resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def get_bucket_acl(self):
        url = self._conf.uri("?acl")
        table = PrettyTable([self._conf._bucket, ""])
        table.align = "l"
        table.padding_width = 3
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"get resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                try:
                    table.add_row(['ACL', ("%s: %s" % (grant.getElementsByTagName("ID")[
                                  0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                except Exception:
                    table.add_row(['ACL', ("%s: %s" % (
                        'anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
            if rt.status_code == 200:
                try:
                    print(unicode(table))
                except Exception as e:
                    print(table)
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def put_bucket_versioning(self, status):
        url = self._conf.uri("?versioning")
        try:
            data = '''
        <VersioningConfiguration>
  <Status>{status}</Status>
</VersioningConfiguration>
'''.format(status=status)
            rt = self._session.put(
                url=url, auth=CosS3Auth(self._conf), data=data)
            logger.debug(u"put resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def get_bucket_versioning(self):
        url = self._conf.uri("?versioning")
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            if rt.status_code == 200:
                try:
                    root = minidom.parseString(rt.content).documentElement
                    status = root.getElementsByTagName(
                        "Status")[0].childNodes[0].data
                except Exception:
                    status = "Not configured"
                logger.info(status)
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def probe(self, **kwargs):
        test_num = int(kwargs['test_num'])
        filesize = int(kwargs['file_size'])
        filename = "tmp_test_" + str(filesize) + "M"
        time_upload = 0
        time_download = 0
        max_time_upload = 0
        max_time_download = 0
        min_time_upload = float("inf")
        min_time_download = float("inf")
        succ_num = 0
        rt = gen_local_file(filename, filesize)
        if 0 != rt:
            logger.warn("Create testfile failed")
            logger.info("[failure]")
            return -1
        for i in range(test_num):
            kw = {
                "skipmd5": True,
                "sync": False,
                "force": True,
                "include": "*",
                "ignore": ""}
            time_start = time.time()
            rt = self.upload_file(filename, filename, **kw)
            time_end = time.time()
            tmp_time = (time_end - time_start)
            max_time_upload = max(max_time_upload, tmp_time)
            min_time_upload = min(min_time_upload, tmp_time)
            time_upload += tmp_time
            if 0 != rt:
                logger.info("[failure]")
                continue
            logger.info("[success]")
            time_start = time.time()
            kw = {
                "force": True,
                "sync": False,
                "num": 10,
                "include": "*",
                "ignore": ""}
            rt = self.download_file(filename, filename, **kw)
            time_end = time.time()
            tmp_time = (time_end - time_start)
            max_time_download = max(max_time_download, tmp_time)
            min_time_download = min(min_time_download, tmp_time)
            time_download += tmp_time
            if 0 != rt:
                logger.info("[failure]")
                continue
            logger.info("[success]")
            succ_num += 1
        logger.info("Success Rate: [{succ_num}/{test_num}]".format(
            succ_num=int(succ_num), test_num=int(test_num)))
        os.remove(filename)
        if succ_num == test_num:
            table = PrettyTable(
                [str(filesize) + "M TEST", "Average", "Min", "Max"])
            table.align = "l"
            table.padding_width = 3
            table.header = True
            table.border = False
            avg_upload_widthband = change_to_human(
                float(filesize) * succ_num / float(time_upload) * 1024 * 1024) + "B/s"
            avg_download_widthband = change_to_human(
                float(filesize) * succ_num / float(time_download) * 1024 * 1024) + "B/s"
            min_upload_widthband = change_to_human(
                float(filesize) / float(max_time_upload) * 1024 * 1024) + "B/s"
            min_download_widthband = change_to_human(
                float(filesize) / float(max_time_download) * 1024 * 1024) + "B/s"
            max_upload_widthband = change_to_human(
                float(filesize) / float(min_time_upload) * 1024 * 1024) + "B/s"
            max_download_widthband = change_to_human(
                float(filesize) / float(min_time_download) * 1024 * 1024) + "B/s"
            table.add_row(['Upload', avg_upload_widthband,
                          min_upload_widthband, max_upload_widthband])
            table.add_row(['Download', avg_download_widthband,
                          min_download_widthband, max_download_widthband])
            logger.info(table)
            return 0
        return -1


class CosS3Client(object):

    def __init__(self, conf):
        self._conf = conf
        self._session = requests.session()

    def op_int(self):
        return Interface(conf=self._conf, session=self._session)


if __name__ == "__main__":
    pass
