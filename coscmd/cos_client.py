# -*- coding=utf-8
from prettytable import PrettyTable
from os import path
from contextlib import closing
from xml.dom import minidom
from six import text_type
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
from tqdm import tqdm
from logging.handlers import RotatingFileHandler
from wsgiref.handlers import format_date_time
import qcloud_cos

if sys.version > '3':
    from coscmd.cos_auth import CosS3Auth
    from coscmd.cos_threadpool import SimpleThreadPool
    from coscmd.cos_comm import to_bytes, to_unicode, get_file_md5
else:
    from cos_auth import CosS3Auth
    from cos_threadpool import SimpleThreadPool
    from cos_comm import to_bytes, to_unicode, get_file_md5

logger = logging.getLogger("coscmd")
logger_sdk = logging.getLogger("qcloud_cos.cos_client")
handle_sdk = logging.StreamHandler()
handle_sdk.setLevel(logging.WARN)
logger_sdk.addHandler(handle_sdk)


def to_printable_str(s):
    if isinstance(s, text_type):
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
        sys.stdout.write(question + prompt)
        sys.stdout.flush()
        if sys.version > '3':
            choice = input()
        else:
            choice = raw_input()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


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
    except:
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


class CoscmdConfig(object):

    def __init__(self, appid, region, endpoint, bucket, secret_id, secret_key, part_size=1, max_thread=5, schema='https', anonymous=False, verify='md5', *args, **kwargs):
        self._appid = appid
        self._region = region
        self._endpoint = endpoint
        self._bucket = bucket
        self._secret_id = secret_id
        self._secret_key = secret_key
        self._part_size = part_size
        self._max_thread = max_thread
        self._schema = schema
        self._anonymous = anonymous
        self._verify = verify
        self._endpoint = endpoint
        logger.debug("config parameter-> appid: {appid}, region: {region}, endpoint: {endpoint}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
            appid=appid,
            region=region,
            endpoint=endpoint,
            bucket=bucket,
            part_size=part_size,
            max_thread=max_thread))

    def uri(self, path=None):
        if path:
            if self._region is not None:
                url = u"{schema}://{bucket}-{uid}.cos.{region}.myqcloud.com/{path}".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    uid=self._appid,
                    region=self._region,
                    path=to_unicode(path)
                )
            else:
                url = u"{schema}://{bucket}-{uid}.{endpoint}/{path}".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    uid=self._appid,
                    endpoint=self._endpoint,
                    path=to_unicode(path)
                )
        else:
            if self._region is not None:
                url = u"{schema}://{bucket}-{uid}.cos.{region}.myqcloud.com/".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    uid=self._appid,
                    region=self._region
                )
            else:
                url = u"{schema}://{bucket}-{uid}.{endpoint}/".format(
                    schema=self._schema,
                    bucket=self._bucket,
                    uid=self._appid,
                    endpoint=self._endpoint
                )

        url = url.replace("+", "%2B")
        return url


class Interface(object):

    def __init__(self, conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = {}
        self._have_finished = 0
        self._err_tips = ''
        self._retry = 2
        self._file_num = 0
        self._folder_num = 0
        self._fail_num = 0
        self._path_md5 = ""
        self._have_uploaded = []
        self._etag = 'ETag'
        self._pbar = ''
        self._inner_threadpool = SimpleThreadPool(1)
        self._multiupload_threshold = 5 * 1024 * 1024 + 1024
        self._multidownload_threshold = 5 * 1024 * 1024 + 1024
        if conf._endpoint == "":
            sdk_config = qcloud_cos.CosConfig(Region=conf._region,
                                              SecretId=conf._secret_id,
                                              SecretKey=conf._secret_key,
                                              Scheme=conf._schema,
                                              Anonymous=conf._anonymous)
        else:
            sdk_config = qcloud_cos.CosConfig(Endpoint=conf._endpoint,
                                              Region=conf._region,
                                              SecretId=conf._secret_id,
                                              SecretKey=conf._secret_key,
                                              Scheme=conf._schema,
                                              Anonymous=conf._anonymous)
        self._client = qcloud_cos.CosS3Client(sdk_config)
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

    def check_file_md5(self, _local_path, _cos_path, _md5):
        url = self._conf.uri(path=quote(to_printable_str(_cos_path)))
        rt = self._session.head(
            url=url, auth=CosS3Auth(self._conf), stream=True)
        if rt.status_code != 200:
            return False
        tmp = os.stat(_local_path)
        if tmp.st_size != int(rt.headers['Content-Length']):
            return False
        else:
            if 'x-cos-meta-md5' not in rt.headers or _md5 != rt.headers['x-cos-meta-md5']:
                return False
            else:
                return True

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
                        self._md5[int(ID)] = content.getElementsByTagName(
                            self._etag)[0].childNodes[0].data[1:-1]
                else:
                    logger.debug(response_info(rt))
                    logger.debug("list parts error")
                    return False
        except Exception:
            logger.debug("list parts error")
            return False
        return True

    def upload_folder(self, local_path, cos_path, _http_headers='', **kwargs):

        def recursive_upload_folder(_local_path, _cos_path):
            _local_path = to_unicode(_local_path)
            _cos_path = to_unicode(_cos_path)
            filelist = os.listdir(_local_path)
            if _cos_path.endswith('/') is False:
                _cos_path += "/"
            if _local_path.endswith('/') is False:
                _local_path += '/'
            _cos_path = _cos_path.lstrip('/')
            for filename in filelist:
                try:
                    filepath = os.path.join(_local_path, filename)
                    if os.path.isdir(filepath):
                        recursive_upload_folder(filepath, _cos_path + filename)
                    else:
                        file_size = os.path.getsize(filepath)
                        if file_size <= self._multiupload_threshold:
                            self._inner_threadpool.add_task(
                                self.single_upload, filepath, _cos_path + filename, _http_headers, **kwargs)
                        else:
                            multiupload_filelist.append(
                                [filepath, _cos_path + filename])
                except Exception as e:
                    self._fail_num += 1
                    logger.warn(e)
                    logger.warn("Upload {file} error".format(
                        file=to_printable_str(filename)))

        self._fail_num = 0
        multiupload_filelist = []
        self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
        recursive_upload_folder(local_path, cos_path)
        self._inner_threadpool.wait_completion()
        result = self._inner_threadpool.get_result()
        _success_num = 0
        _skip_num = 0
        _fail_num = self._fail_num
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
        logger.info(u"{success_files} files successful, {skip_files} files skipped, {fail_files} files failed"
                    .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        if _fail_num == 0:
            return 0
        else:
            return -1

    def single_upload(self, local_path, cos_path, _http_headers='{}', **kwargs):
        logger.info(u"Upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        _md5 = ""
        _http_header = yaml.safe_load(_http_headers)
        for rule in kwargs['ignore']:
            if fnmatch.fnmatch(local_path, rule) is True:
                logger.info(u"This file matches the ignore rule, skip upload")
                return -2

        file_size = os.path.getsize(local_path)
        if kwargs['skipmd5'] is False:
            if file_size > 5 * 1024 * 1024 * 1024:
                logger.info(
                    u"MD5 is being calculated, please wait. If you do not need to calculate md5, you can use --skipmd5 to skip")
            _md5 = get_file_md5(local_path)

        # -2 means skipfile
        if kwargs['sync'] is True:
            if self.check_file_md5(local_path, cos_path, _md5):
                logger.info(
                    u"The file on cos is the same as the local file, skip upload")
                return -2
        try:
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as File:
                    data = File.read()
        except Exception as e:
            logger.warn(e)
            return 0
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        for j in range(self._retry):
            try:
                http_header = _http_header
                http_header['x-cos-meta-md5'] = _md5
                rt = self._session.put(url=url,
                                       auth=CosS3Auth(self._conf), data=data, headers=http_header)
                if rt.status_code == 200:
                    return 0
                else:
                    time.sleep(2**j)
                    logger.warn(response_info(rt))
                    continue
                if j + 1 == self._retry:
                    return -1
            except Exception as e:
                logger.warn(e)
                logger.warn(u"Upload file failed")
        return -1

    def multipart_upload(self, local_path, cos_path, _http_headers='{}', **kwargs):

        def init_multiupload():
            url = self._conf.uri(path=quote(to_printable_str(cos_path)))
            self._md5 = {}
            self.c = 0
            self._have_uploaded = []
            self._upload_id = None
            self._path_md5 = get_md5_filename(local_path, cos_path)
            logger.debug("init with : " + url)
            if os.path.isfile(self._path_md5):
                with open(self._path_md5, 'rb') as f:
                    self._upload_id = f.read()
                if self.list_part(cos_path) is True:
                    logger.info(u"Continue uploading from last breakpoint")
                    return 0
            http_header = _http_header
            http_header['x-cos-meta-md5'] = _md5
            rt = self._session.post(
                url=url + "?uploads", auth=CosS3Auth(self._conf), headers=http_header)
            logger.debug("Init resp, status code: {code}, headers: {headers}, text: {text}".format(
                code=rt.status_code,
                headers=rt.headers,
                text=to_printable_str(rt.text)))

            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                self._upload_id = root.getElementsByTagName(
                    "UploadId")[0].childNodes[0].data
                if os.path.isdir(os.path.expanduser("~/.tmp")) is False:
                    os.makedirs(os.path.expanduser("~/.tmp"))
                with open(self._path_md5, 'wb') as f:
                    f.write(to_bytes(self._upload_id))
                return 0
            else:
                logger.warn(response_info(rt))
                return -1
            return 0

        def multiupload_parts():

            def multiupload_parts_data(local_path, offset, length, parts_size, idx):
                try:
                    with open(local_path, 'rb') as File:
                        File.seek(offset, 0)
                        data = File.read(length)
                except Exception as e:
                    logger.warn(e)
                    return -1
                url = self._conf.uri(path=quote(to_printable_str(
                    cos_path))) + "?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx, uploadid=self._upload_id)
                for j in range(self._retry):
                    http_header = _http_header
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf),
                                           data=data, headers=http_header)
                    logger.debug("Multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx,
                        round=j + 1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=to_printable_str(rt.text)))
                    try:
                        self._md5[idx] = rt.headers[self._etag][1:-1]
                    except:
                        self._md5[idx] = ""
                    if rt.status_code == 200:
                        if self._conf._verify == "sha1":
                            local_encryption = sha1(data).hexdigest()
                        else:
                            local_encryption = md5(data).hexdigest()
                        logger.debug("cos encryption: {key}".format(
                            key=self._md5[idx]))
                        logger.debug("local encryption: {key}".format(
                            key=local_encryption))
                        if (kwargs['skipmd5'] or self._md5[idx] == local_encryption):
                            self._have_finished += 1
                            self._pbar.update(length)
                            break
                        else:
                            logger.warn(
                                "Encryption verification is inconsistent")
                            continue
                    else:
                        logger.warn(response_info(rt))
                        time.sleep(2**j)
                        continue
                    if j + 1 == self._retry:
                        logger.warn("Upload part failed: part{part}, round{round}, code: {code}".format(
                            part=idx, round=j + 1, code=rt.status_code))
                        return -1
                return 0

            offset = 0
            file_size = path.getsize(local_path)
            logger.debug("file size: " + str(file_size))
            chunk_size = 1024 * 1024 * self._conf._part_size
            while file_size / chunk_size > 8000:
                chunk_size = chunk_size * 10
            parts_num = int(file_size / chunk_size)
            last_size = file_size - parts_num * chunk_size
            self._have_finished = len(self._have_uploaded)
            if last_size != 0:
                parts_num += 1
            _max_thread = min(self._conf._max_thread,
                              parts_num - self._have_finished)
            pool = SimpleThreadPool(_max_thread)

            logger.debug("chunk_size: " + str(chunk_size))
            logger.debug('Upload file concurrently')
            self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
            for i in range(parts_num):
                if(str(i + 1) in self._have_uploaded):
                    offset += chunk_size
                    self._pbar.update(chunk_size)
                    continue
                if i + 1 == parts_num:
                    pool.add_task(multiupload_parts_data, local_path,
                                  offset, file_size - offset, parts_num, i + 1)
                else:
                    pool.add_task(multiupload_parts_data, local_path,
                                  offset, chunk_size, parts_num, i + 1)
                    offset += chunk_size
            pool.wait_completion()
            result = pool.get_result()
            self._pbar.close()
            if result['success_all']:
                return 0
            else:
                return -1

        def complete_multiupload():
            logger.info('Completing multiupload, please wait')
            doc = minidom.Document()
            root = doc.createElement("CompleteMultipartUpload")
            list_md5 = sorted(self._md5.items(), key=lambda d: d[0])
            for i, v in list_md5:
                t = doc.createElement("Part")
                t1 = doc.createElement("PartNumber")
                t1.appendChild(doc.createTextNode(str(i)))
                t2 = doc.createElement(self._etag)
                t2.appendChild(doc.createTextNode('"{v}"'.format(v=v)))
                t.appendChild(t1)
                t.appendChild(t2)
                root.appendChild(t)
                data = root.toxml()
                url = self._conf.uri(path=quote(to_printable_str(
                    cos_path))) + "?uploadId={uploadid}".format(uploadid=self._upload_id)
                logger.debug('complete url: ' + url)
                logger.debug("complete data: " + data)
            try:
                with closing(self._session.post(url, auth=CosS3Auth(self._conf), data=data, stream=True)) as rt:
                    logger.debug("complete status code: {code}".format(
                        code=rt.status_code))
                    logger.debug("complete headers: {headers}".format(
                        headers=rt.headers))
                if rt.status_code == 200:
                    os.remove(self._path_md5)
                    return 0
                else:
                    logger.warn(response_info(rt))
                    return -1
            except Exception:
                return -1

        logger.info(u"Upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        _md5 = ""
        _http_header = yaml.safe_load(_http_headers)
        for rule in kwargs['ignore']:
            if fnmatch.fnmatch(local_path, rule) is True:
                logger.info(u"This file matches the ignore rule, skip upload")
                return -2

        file_size = os.path.getsize(local_path)
        if kwargs['skipmd5'] is False:
            if file_size > 5 * 1024 * 1024 * 1024:
                logger.info(
                    u"MD5 is being calculated, please wait. If you do not need to calculate md5, you can use --skipmd5 to skip")
            _md5 = get_file_md5(local_path)

        if kwargs['sync'] is True:
            if self.check_file_md5(local_path, cos_path, _md5):
                logger.info(
                    u"The file on cos is the same as the local file, skip upload")
                return -2
        rt = init_multiupload()
        if not rt:
            logger.debug(u"Init multipart upload ok")
        else:
            logger.debug(u"Init multipart upload failed")
            return -1
        rt = multiupload_parts()
        if not rt:
            logger.debug(u"Multipart upload ok")
        else:
            logger.warn(
                u"Some partial upload failed. Please retry the last command to continue.")
            return -1
        rt = complete_multiupload()
        if not rt:
            logger.debug(u"Complete multipart upload ok")
        else:
            logger.warn(u"Complete multipart upload failed")
            return -1
        return 0

    def upload_file(self, local_path, cos_path, _http_headers='{}', **kwargs):
        file_size = path.getsize(local_path)
        if file_size <= self._conf._part_size * 1024 * 1024 + 1024 or file_size <= self._multiupload_threshold:
            return self.single_upload(local_path, cos_path, _http_headers, **kwargs)
        else:
            return self.multipart_upload(local_path, cos_path, _http_headers, **kwargs)

    def copy_folder(self, source_path, cos_path):

        source_schema = source_path.split('/')[0] + '/'
        source_path = source_path[len(source_schema):]
        NextMarker = ""
        IsTruncated = "true"
        _file_num = 0
        _success_num = 0
        _fail_num = 0
        while IsTruncated == "true":
            tmp_url = '?prefix={prefix}&marker={nextmarker}'.format(
                prefix=quote(to_printable_str(source_path)),
                nextmarker=quote(to_printable_str(NextMarker)))
            url = self._conf._schema + "://" + source_schema + tmp_url
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName(
                    "IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName(
                        "NextMarker")[0].childNodes[0].data
                fileset = root.getElementsByTagName("Contents")
                for _file in fileset:
                    _tmp = _file.getElementsByTagName(
                        "Key")[0].childNodes[0].data
                    _source_path = source_schema + _tmp
                    if source_path.endswith('/') is False and len(source_path) != 0:
                        _cos_path = cos_path + _tmp[len(source_path) + 1:]
                    else:
                        _cos_path = cos_path + _tmp[len(source_path):]
                    _cos_path = to_unicode(_cos_path)
                    _source_path = to_unicode(_source_path)
                    if _cos_path.endswith('/'):
                        continue
                    _file_num += 1
                    if self.copy_file(_source_path, _cos_path):
                        _success_num += 1
                    else:
                        _fail_num += 1
            else:
                logger.warn(response_info(rt))
                return False
        if _file_num == 0:
            logger.info(u"The directory does not exist")
            return False
        logger.info(u"Copy {success_files} files successful, {fail_files} files failed"
                    .format(success_files=_success_num, fail_files=_fail_num))
        if _file_num == _success_num:
            return True
        else:
            return False

    def copy_file(self, source_path, cos_path):

        def single_copy():
            url = self._conf.uri(path=quote(to_printable_str(cos_path)))
            for j in range(self._retry):
                try:
                    http_header = dict()
                    http_header['x-cos-copy-source'] = source_path
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf), headers=http_header)
                    if rt.status_code == 200:
                        logger.info(u"Copy {source_path}   =>   cos://{bucket}/{cos_path}  [100%]".format(
                            bucket=self._conf._bucket,
                            source_path=source_path,
                            cos_path=cos_path))
                        return True
                    else:
                        time.sleep(2**j)
                        logger.warn(response_info(rt))
                        continue
                    if j + 1 == self._retry:
                        return False
                except Exception as e:
                    logger.warn(e)
                    logger.warn(u"Copy file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=quote(to_printable_str(cos_path)))
            self._md5 = {}
            self._have_finished = 0
            self._upload_id = None
            http_header = dict()
            rt = self._session.post(
                url=url + "?uploads", auth=CosS3Auth(self._conf), headers=http_header)
            logger.debug(u"Init resp, status code: {code}, headers: {headers}, text: {text}".format(
                code=rt.status_code,
                headers=rt.headers,
                text=rt.text))

            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                self._upload_id = root.getElementsByTagName(
                    "UploadId")[0].childNodes[0].data
                return True
            else:
                logger.warn(response_info(rt))
                return False
            return True

        def copy_parts(file_size):

            def source_path_parser():
                # <Bucketname>-<APPID>.cos.<Region>.myqcloud.com/filepath
                try:
                    tmp = source_path.split('.')
                    source_bucket = tmp[0]
                    source_appid = source_bucket.split('-')[1]
                    source_bucket = source_bucket.split('-')[0]
                    source_region = tmp[2]
                    source_cospath = tmp[-1]
                except Exception:
                    logger.warn(u"Source path format error")
                return source_bucket, source_appid, source_region, source_cospath

            def copy_parts_data(source_path, offset, length, parts_size, idx):
                url = self._conf.uri(path=quote(to_printable_str(
                    cos_path))) + "?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx, uploadid=self._upload_id)
                http_header = dict()
                http_header['x-cos-copy-source'] = source_path
                http_header['x-cos-copy-source-range'] = "bytes=" + \
                    str(offset) + "-" + str(offset + length - 1)
                for j in range(self._retry):
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf),
                                           headers=http_header)
                    logger.debug(u"Copy part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx,
                        round=j + 1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=rt.text))
                    root = minidom.parseString(rt.content).documentElement
                    self._md5[idx] = root.getElementsByTagName(
                        "ETag")[0].childNodes[0].data
                    if rt.status_code == 200:
                        self._have_finished += 1
                        self._pbar.update(length)
                        break
                    else:
                        logger.warn(response_info(rt))
                        time.sleep(2**j)
                        continue
                    if j + 1 == self._retry:
                        logger.warn(u"Upload part failed: part{part}, round{round}, code: {code}".format(
                            part=idx, round=j + 1, code=rt.status_code))
                        return False
                return True

            offset = 0
            logger.debug("file size: " + str(file_size))
            chunk_size = 1024 * 1024 * self._conf._part_size
            while file_size / chunk_size > 8000:
                chunk_size = chunk_size * 10
            parts_num = file_size / chunk_size
            last_size = file_size - parts_num * chunk_size
            if last_size != 0:
                parts_num += 1
            _max_thread = min(self._conf._max_thread, parts_num)
            pool = SimpleThreadPool(_max_thread)

            logger.debug(u"chunk_size: " + str(chunk_size))
            logger.debug(u'copy file concurrently')
            logger.info(u"Copy {source_path}   =>   cos://{bucket}/{cos_path}".format(
                bucket=self._conf._bucket,
                source_path=source_path,
                cos_path=cos_path))
            self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
            for i in range(parts_num):
                if i + 1 == parts_num:
                    pool.add_task(copy_parts_data, source_path,
                                  offset, file_size - offset, parts_num, i + 1)
                else:
                    pool.add_task(copy_parts_data, source_path,
                                  offset, chunk_size, parts_num, i + 1)
                    offset += chunk_size

            pool.wait_completion()
            result = pool.get_result()
            self._pbar.close()
            if result['success_all']:
                return True
            else:
                return False

        def complete_multiupload():
            logger.info(u"Completing multicopy, please wait")
            doc = minidom.Document()
            root = doc.createElement("CompleteMultipartUpload")
            list_md5 = sorted(self._md5.items(), key=lambda d: d[0])
            for i, v in list_md5:
                t = doc.createElement("Part")
                t1 = doc.createElement("PartNumber")
                t1.appendChild(doc.createTextNode(str(i)))
                t2 = doc.createElement("ETag")
                t2.appendChild(doc.createTextNode('{v}'.format(v=v)))
                t.appendChild(t1)
                t.appendChild(t2)
                root.appendChild(t)
                data = root.toxml()
                url = self._conf.uri(path=quote(to_printable_str(
                    cos_path))) + "?uploadId={uploadid}".format(uploadid=self._upload_id)
                logger.debug(u"Complete url: " + url)
                logger.debug(u"Complete data: " + data)
            try:
                with closing(self._session.post(url, auth=CosS3Auth(self._conf), data=data, stream=True)) as rt:
                    logger.debug(u"Complete status code: {code}".format(
                        code=rt.status_code))
                    logger.debug(u"Complete headers: {headers}".format(
                        headers=rt.headers))
                if rt.status_code == 200:
                    return True
                else:
                    logger.warn(response_info(rt))
                    return False
            except Exception:
                return False
            return True
        try:
            source_path = quote(to_printable_str(source_path))
            rt = self._session.head(
                url="http://" + source_path, auth=CosS3Auth(self._conf))
            if rt.status_code != 200:
                logger.warn(u"Copy sources do not exist")
                return False
            file_size = int(rt.headers['Content-Length'])
            if file_size < self._conf._part_size * 1024 * 1024 + 1024:
                for _ in range(self._retry):
                    if single_copy() is True:
                        return True
                return False
            else:
                for _ in range(self._retry):
                    rt = init_multiupload()
                    if rt:
                        break
                else:
                    return False
                logger.debug(u"Init multipart copy ok")
                rt = copy_parts(file_size=file_size)
                if rt is False:
                    return False
                logger.debug(u"Multipart copy ok")
                for _ in range(self._retry):
                    rt = complete_multiupload()
                    if rt:
                        logger.debug(u"Complete multipart copy ok")
                        return True
                logger.warn(u"Complete multipart copy failed")
                return False
        except Exception as e:
            logger.warn(e)
            return False

    def delete_folder(self, cos_path, **kwargs):

        _force = kwargs['force']
        _versions = kwargs['versions']
        cos_path = to_unicode(cos_path)
        if cos_path == "/":
            cos_path = ""
        # make sure
        if _force is False:
            if query_yes_no(u"WARN: you are deleting all files under cos_path '{cos_path}', please make sure".format(cos_path=cos_path)) is False:
                return False
        kwargs['force'] = True
        self._have_finished = 0
        self._fail_num = 0
        NextMarker = ""
        IsTruncated = "true"
        if _versions:
            NextMarker = "/"
            NextVersionMarker = "/"
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
                            Bucket=self._conf._bucket + "-" + self._conf._appid,
                            KeyMarker=KeyMarker,
                            VersionIdMarker=VersionIdMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(e)
                    if i + 1 == self._retry:
                        return False
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
                    rt = self._client.delete_objects(Bucket=self._conf._bucket + "-" + self._conf._appid,
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
            NextMarker = "/"
            while IsTruncated == "true":
                deleteList = {}
                deleteList['Object'] = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects(
                            Bucket=self._conf._bucket + "-" + self._conf._appid,
                            Marker=NextMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(e)
                    if i + 1 == self._retry:
                        return False
                if 'IsTruncated' in rt:
                    IsTruncated = rt['IsTruncated']
                if 'NextMarker' in rt:
                    NextMarker = rt['NextMarker']
                if 'Contents' in rt:
                    for _file in rt['Contents']:
                        _path = _file['Key']
                        deleteList['Object'].append({'Key': _path})
                if len(deleteList['Object']) > 0:
                    rt = self._client.delete_objects(Bucket=self._conf._bucket + "-" + self._conf._appid,
                                                     Delete=deleteList)
                if 'Deleted' in rt:
                    self._have_finished += len(rt['Deleted'])
                    self._file_num += len(rt['Deleted'])
                    for file in rt['Deleted']:
                        logger.info(u"Delete {file}".format(file=file['Key']))
                if 'Error' in rt:
                    self._file_num += len(rt['Error'])
                    for file in rt['Error']:
                        logger.info(u"Delete {file} fail, code: {code}, msg: {msg}"
                                    .format(file=file['Key'],
                                            code=file['Code'],
                                            msg=file['Message']))
        # delete the remaining files
        logger.info(u"Delete the remaining files again")
        if self._file_num == 0:
            logger.info(u"The directory does not exist")
            return False
        self.delete_folder_redo(cos_path, **kwargs)
        self._fail_num = self._file_num - self._have_finished
        logger.info(u"{files} files successful, {fail_files} files failed"
                    .format(files=self._have_finished, fail_files=self._fail_num))
        if self._file_num == self._have_finished:
            return True
        else:
            return False

    def delete_folder_redo(self, cos_path, **kwargs):
        _force = kwargs['force']
        _versions = kwargs['versions']
        cos_path = to_unicode(cos_path)
        if cos_path == "/":
            cos_path = ""
        NextMarker = ""
        IsTruncated = "true"
        if _versions:
            NextMarker = "/"
            NextVersionMarker = "/"
            KeyMarker = ""
            VersionIdMarker = ""
            while IsTruncated == "true":
                deleteList = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects_versions(
                            Bucket=self._conf._bucket + "-" + self._conf._appid,
                            KeyMarker=KeyMarker,
                            VersionIdMarker=VersionIdMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(e)
                    if i + 1 == self._retry:
                        return False
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
                                Bucket=self._conf._bucket + "-" + self._conf._appid,
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
            NextMarker = "/"
            while IsTruncated == "true":
                deleteList = []
                for i in range(self._retry):
                    try:
                        rt = self._client.list_objects(
                            Bucket=self._conf._bucket + "-" + self._conf._appid,
                            Marker=NextMarker,
                            MaxKeys=1000,
                            Prefix=cos_path,
                        )
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.warn(e)
                    if i + 1 == self._retry:
                        return False
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
                                Bucket=self._conf._bucket + "-" + self._conf._appid,
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

    def delete_file(self, cos_path, **kwargs):
        if kwargs['force'] is False:
            if query_yes_no(u"WARN: you are deleting the file in the '{cos_path}' cos_path, please make sure".format(cos_path=cos_path)) is False:
                return False
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
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def list_multipart(self, cos_path):
        NextMarker = ""
        IsTruncated = "true"
        _success_num = 0
        _fail_num = 0
        cos_path = to_printable_str(cos_path)
        try:
            while IsTruncated == "true":
                table = PrettyTable(["Path", "Size/Type", "Time"])
                table.align = "l"
                table.align['Size/Type'] = 'r'
                table.padding_width = 3
                table.header = False
                table.border = False
                url = self._conf.uri(path='?uploads&prefix={prefix}&marker={nextmarker}'
                                     .format(prefix=quote(to_printable_str(cos_path)), nextmarker=quote(to_printable_str(NextMarker))))
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
                if rt.status_code == 200:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName(
                        "IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName(
                            "NextMarker")[0].childNodes[0].data
                    logger.debug(u"init resp, status code: {code}, headers: {headers}, text: {text}".format(
                        code=rt.status_code,
                        headers=rt.headers,
                        text=rt.text))
                    fileset = root.getElementsByTagName("Upload")
                    for _file in fileset:
                        self._file_num += 1
                        _key = _file.getElementsByTagName(
                            "Key")[0].childNodes[0].data
                        _uploadid = _file.getElementsByTagName(
                            "UploadId")[0].childNodes[0].data
                        logger.info(u"Key:{key}, UploadId:{uploadid}".format(
                            key=_key, uploadid=_uploadid))
                else:
                    logger.warn(response_info(rt))
                    return False
            return True
        except Exception as e:
            logger.warn(e)
            return False

    def abort_parts(self, cos_path):
        NextMarker = ""
        IsTruncated = "true"
        _success_num = 0
        _fail_num = 0
        cos_path = to_printable_str(cos_path)
        try:
            while IsTruncated == "true":
                table = PrettyTable(["Path", "Size/Type", "Time"])
                table.align = "l"
                table.align['Size/Type'] = 'r'
                table.padding_width = 3
                table.header = False
                table.border = False
                url = self._conf.uri(path='?uploads&prefix={prefix}&marker={nextmarker}'
                                     .format(prefix=quote(to_printable_str(cos_path)), nextmarker=quote(to_printable_str(NextMarker))))
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
                if rt.status_code == 200:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName(
                        "IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName(
                            "NextMarker")[0].childNodes[0].data
                    logger.debug(u"init resp, status code: {code}, headers: {headers}, text: {text}".format(
                        code=rt.status_code,
                        headers=rt.headers,
                        text=rt.text))
                    fileset = root.getElementsByTagName("Upload")
                    for _file in fileset:
                        self._file_num += 1
                        _key = _file.getElementsByTagName(
                            "Key")[0].childNodes[0].data
                        _uploadid = _file.getElementsByTagName(
                            "UploadId")[0].childNodes[0].data
                        _url = self._conf.uri(path="{key}?uploadId={uploadid}".format(
                            key=quote(to_printable_str(_key)), uploadid=_uploadid))
                        _rt = self._session.delete(
                            url=_url, auth=CosS3Auth(self._conf))
                        if _rt.status_code == 204:
                            logger.info(u"Aborting part, Key:{key}, UploadId:{uploadid}".format(
                                key=_key, uploadid=_uploadid))
                            _success_num += 1
                        else:
                            logger.info(u"Aborting part, Key:{key}, UploadId:{uploadid} fail".format(
                                key=_key, uploadid=_uploadid))
                            _fail_num += 1
                else:
                    logger.warn(response_info(rt))
                    return False
            logger.info(u"{files} files successful, {fail_files} files failed"
                        .format(files=_success_num, fail_files=_fail_num))
            if _fail_num == 0:
                return True
            else:
                return False
        except Exception as e:
            logger.warn(e)
            return False

    def list_objects(self, cos_path, **kwargs):
        try:
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
                                self._conf._bucket + "-" + self._conf._appid,
                                Delimiter=Delimiter,
                                KeyMarker=KeyMarker,
                                VersionIdMarker=VersionIdMarker,
                                MaxKeys=1000,
                                Prefix=cos_path,
                            )
                            break
                        except Exception as e:
                            time.sleep(1 << i)
                            logger.warn(e)
                        if i + 1 == self._retry:
                            return False
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
                return True
            else:
                NextMarker = ""
                while IsTruncated == "true":
                    table = PrettyTable(["Path", "Size/Type", "Time"])
                    table.align = "l"
                    table.align['Size/Type'] = 'r'
                    table.padding_width = 3
                    table.header = False
                    table.border = False
                    for i in range(self._retry):
                        try:
                            rt = self._client.list_objects(
                                self._conf._bucket + "-" + self._conf._appid,
                                Delimiter=Delimiter,
                                Marker=NextMarker,
                                MaxKeys=1000,
                                Prefix=cos_path
                            )
                            break
                        except Exception as e:
                            time.sleep(1 << i)
                            logger.warn(e)
                        if i + 1 == self._retry:
                            return False
                    if 'IsTruncated' in rt:
                        IsTruncated = rt['IsTruncated']
                    if 'NextMarker' in rt:
                        NextMarker = rt['NextMarker']
                    if 'CommonPrefixes' in rt:
                        for _folder in rt['CommonPrefixes']:
                            _time = ""
                            _type = "DIR"
                            _path = _folder['Prefix']
                            table.add_row([_path, _type, _time])
                    if 'Contents' in rt:
                        for _file in rt['Contents']:
                            self._file_num += 1
                            _time = _file['LastModified']
                            _time = time.localtime(utc_to_local(_time))
                            _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                            _size = _file['Size']
                            self._total_size += int(_size)
                            if _human is True:
                                _size = change_to_human(_size)
                            _path = _file['Key']
                            table.add_row([_path, _size, _time])
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
                return True
        except Exception as e:
            print(e)

    def info_object(self, cos_path, _human=False):
        table = PrettyTable([cos_path, ""])
        table.align = "l"
        table.padding_width = 3
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        logger.info("Info with : " + url)
        try:
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"info resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                _size = rt.headers['Content-Length']
                if _human is True:
                    _size = change_to_human(_size)
                _time = time.localtime(utc_to_local(
                    rt.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S GMT'))
                _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                table.add_row(['File size', _size])
                table.add_row(['Last mod', _time])
                url = self._conf.uri(cos_path + "?acl")
                try:
                    rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
                    logger.debug(u"get resp, status code: {code}, headers: {headers}".format(
                        code=rt.status_code,
                        headers=rt.headers))
                    if rt.status_code == 200:
                        root = minidom.parseString(rt.content).documentElement
                        grants = root.getElementsByTagName("Grant")
                        for grant in grants:
                            try:
                                table.add_row(['ACL', ("%s: %s" % (grant.getElementsByTagName("ID")[0].childNodes[0].data,
                                                                   grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                            except Exception:
                                table.add_row(['ACL', ("%s: %s" % (
                                    'anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                    else:
                        logger.warn(response_info(rt))
                except Exception as e:
                    logger.warn(str(e))
                    return False
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

    def download_folder(self, cos_path, local_path, **kwargs):

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
        cos_path = to_unicode(cos_path)
        multidownload_filelist = []
        self._inner_threadpool = SimpleThreadPool(self._conf._max_thread)
        while IsTruncated == "true":
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}'
                                 .format(prefix=quote(to_printable_str(cos_path)), nextmarker=quote(to_printable_str(NextMarker))))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf))
            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName(
                    "IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName(
                        "NextMarker")[0].childNodes[0].data
                fileset = root.getElementsByTagName("Contents")
                for _file in fileset:
                    try:
                        _cos_path = _file.getElementsByTagName(
                            "Key")[0].childNodes[0].data
                        _size = int(_file.getElementsByTagName(
                            "Size")[0].childNodes[0].data)
                        _local_path = local_path + _cos_path[len(cos_path):]
                        _cos_path = to_unicode(_cos_path)
                        _local_path = to_unicode(_local_path)
                        if _cos_path.endswith('/'):
                            continue
                        if _size <= self._multidownload_threshold:
                            self._inner_threadpool.add_task(
                                self.single_download, _cos_path, _local_path, **kwargs)
                        else:
                            multidownload_filelist.append(
                                [_cos_path, _local_path])
                    except Exception as e:
                        logger.warn(e)
                        logger.warn("Parse xml error")
            else:
                logger.warn(response_info(rt))
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
        for _cos_path, _local_path in multidownload_filelist:
            try:
                rt = self.multipart_download(_cos_path, _local_path, **kwargs)
                if 0 == rt:
                    _success_num += 1
                elif -2 == rt:
                    _skip_num += 1
                else:
                    _fail_num += 1
            except Exception as e:
                print(e)
        logger.info(u"{success_files} files successful, {skip_files} files skipped, {fail_files} files failed"
                    .format(success_files=_success_num, skip_files=_skip_num, fail_files=_fail_num))
        if _fail_num == 0:
            return 0
        else:
            return -1

    # 简单下载
    def single_download(self, cos_path, local_path, **kwargs):
        cos_path = cos_path.lstrip('/')
        logger.info(u"Download cos://{bucket}/{cos_path}   =>   {local_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        for rule in kwargs['ignore']:
            if fnmatch.fnmatch(local_path, rule) is True:
                logger.info(
                    u"This file matches the ignore rule, skip download")
                return -2

        if kwargs['force'] is False:
            if os.path.isfile(local_path) is True:
                if kwargs['sync'] is True:
                    _md5 = get_file_md5(local_path)
                    if self.check_file_md5(local_path, cos_path, _md5):
                        logger.info(
                            u"The file on cos is the same as the local file, skip download")
                        return -2
                else:
                    logger.warn(
                        u"The file {file} already exists, please use -f to overwrite the file".format(file=cos_path))
                    return -1
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        try:
            rt = self._session.get(
                url=url, auth=CosS3Auth(self._conf), stream=True)
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if 'Content-Length' in rt.headers:
                content_len = int(rt.headers['Content-Length'])
            else:
                raise IOError(u"Download failed without Content-Length header")
            if rt.status_code == 200:
                file_len = 0
                dir_path = os.path.dirname(local_path)
                if os.path.isdir(dir_path) is False and dir_path != '':
                    try:
                        os.makedirs(dir_path, 0o755)
                    except Exception:
                        pass
                try:
                    with open(local_path, 'wb') as f:
                        for chunk in rt.iter_content(chunk_size=1024):
                            if chunk:
                                file_len += len(chunk)
                                f.write(chunk)
                        f.flush()
                except Exception as e:
                    logger.warn(u"Fail to write to file")
                    raise Exception(e)
                if file_len != content_len:
                    raise IOError(u"Download failed with incomplete file")
            else:
                raise Exception(response_info(rt))
        except Exception as e:
            logger.warn(str(e))
            os.remove(local_path)
            return -1
        return 0

    # 分块下载
    def multipart_download(self, cos_path, local_path, **kwargs):

        def get_parts_data(local_path, offset, length, parts_size, idx):
            for j in range(self._retry):
                try:
                    time.sleep(1 << j)
                    local_path = local_path + "_" + str(idx)
                    http_header = {}
                    http_header['Range'] = 'bytes=' + \
                        str(offset) + "-" + str(offset + length - 1)
                    rt = self._session.get(url=url, auth=CosS3Auth(
                        self._conf), headers=http_header, stream=True)
                    logger.debug(u"get resp, status code: {code}, headers: {headers}".format(
                        code=rt.status_code,
                        headers=rt.headers))
                    if 'Content-Length' in rt.headers:
                        content_len = int(rt.headers['Content-Length'])
                    else:
                        logger.warn(
                            u"Download failed without Content-Length header")
                        continue
                    if rt.status_code in [206, 200]:
                        file_len = 0
                        dir_path = os.path.dirname(local_path)
                        if os.path.isdir(dir_path) is False and dir_path != '':
                            try:
                                os.makedirs(dir_path, 0o755)
                            except Exception as e:
                                pass
                        with open(local_path, 'wb') as f:
                            for chunk in rt.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    file_len += len(chunk)
                                    f.write(chunk)
                                    self._pbar.update(len(chunk))
                            f.flush()
                        if file_len != content_len:
                            raise IOError(
                                u"Download failed with incomplete file")
                        return 0
                    else:
                        logger.warn(response_info(rt))
                        continue
                except Exception as e:
                    logger.warn(str(e))
                    continue
            return -1
        cos_path = cos_path.lstrip('/')
        logger.info(u"Download cos://{bucket}/{cos_path}   =>   {local_path}".format(
            bucket=self._conf._bucket,
            local_path=local_path,
            cos_path=cos_path))
        for rule in kwargs['ignore']:
            if fnmatch.fnmatch(local_path, rule) is True:
                logger.info(
                    u"This file matches the ignore rule, skip download")
                return -2

        if kwargs['force'] is False:
            if os.path.isfile(local_path) is True:
                if kwargs['sync'] is True:
                    _md5 = get_file_md5(local_path)
                    if self.check_file_md5(local_path, cos_path, _md5):
                        logger.info(
                            u"The file on cos is the same as the local file, skip download")
                        return -2
                else:
                    logger.warn(
                        u"The file {file} already exists, please use -f to overwrite the file".format(file=cos_path))
                    return -1
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        try:
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"download resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                file_size = int(rt.headers['Content-Length'])
            else:
                logger.warn(response_info(rt))
                return -1
        except Exception as e:
            logger.warn(str(e))
            return -1
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        offset = 0
        parts_num = kwargs['num']
        chunk_size = file_size / parts_num
        last_size = file_size - parts_num * chunk_size
        self._have_finished = 0
        if last_size != 0:
            parts_num += 1
        _max_thread = min(self._conf._max_thread,
                          parts_num - self._have_finished)
        pool = SimpleThreadPool(_max_thread)
        logger.debug(u"chunk_size: " + str(chunk_size))
        logger.debug(u'download file concurrently')
        logger.info(u"Downloading {file}".format(file=local_path))
        self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
        for i in range(parts_num):
            if i + 1 == parts_num:
                pool.add_task(get_parts_data, local_path, offset,
                              file_size - offset, parts_num, i + 1)
            else:
                pool.add_task(get_parts_data, local_path, offset,
                              chunk_size, parts_num, i + 1)
                offset += chunk_size
        pool.wait_completion()
        result = pool.get_result()
        self._pbar.close()
        logger.info(u"Completing mget")
        if result['success_all'] is False:
            return -1
        try:
            with open(local_path, 'wb') as f:
                for i in range(parts_num):
                    idx = i + 1
                    file_name = local_path + "_" + str(idx)
                    length = 1024 * 1024
                    offset = 0
                    with open(file_name, 'rb') as File:
                        while (offset < file_size):
                            File.seek(offset, 0)
                            data = File.read(length)
                            f.write(data)
                            offset += length
                    os.remove(file_name)
                f.flush()
        except Exception as e:
            try:
                os.remove(local_path)
            except:
                pass
            for i in range(parts_num):
                idx = i + 1
                file_name = local_path + "_" + str(idx)
                try:
                    os.remove(file_name)
                except:
                    pass
            logger.warn(e)
            logger.warn("Complete file failure")
            return -1
        return 0

    def download_file(self, cos_path, local_path, **kwargs):
        # head操作获取文件大小
        url = self._conf.uri(path=quote(to_printable_str(cos_path)))
        try:
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf))
            logger.debug(u"download resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 200:
                file_size = int(rt.headers['Content-Length'])
            else:
                logger.warn(response_info(rt))
                return -1
        except Exception as e:
            logger.warn(str(e))
            return -1
        try:
            if file_size <= self._conf._part_size * 1024 * 1024 + 1024 or file_size <= self._multidownload_threshold:
                rt = self.single_download(cos_path, local_path, **kwargs)
                return rt
            else:
                rt = self.multipart_download(cos_path, local_path, **kwargs)
                return rt
        except Exception as e:
            logger.warn(e)

    def restore_object(self, cos_path, _day, _tier):
        url = self._conf.uri(path=quote(
            to_printable_str(cos_path)) + "?restore")
        data_xml = '''<RestoreRequest>
   <Days>{day}</Days>
   <CASJobParameters>
     <Tier>{tier}</Tier>
   </CASJobParameters>
</RestoreRequest>'''.format(day=_day, tier=_tier)
        http_header = dict()
        now = datetime.datetime.now()
        stamp = time.mktime(now.timetuple())
        http_header['Date'] = format_date_time(stamp)
        try:
            rt = self._session.post(url=url, auth=CosS3Auth(
                self._conf), data=data_xml, headers=http_header)
            logger.debug(u"init resp, status code: {code}, headers: {headers}".format(
                code=rt.status_code,
                headers=rt.headers))
            if rt.status_code == 202 or rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

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
                if query_yes_no(u"!!!WARN: you are deleting bucket including all objects under this bucket', please make sure!!!") is False:
                    return False
                logger.info("Clearing files and upload parts in the bucket")
                self.abort_parts("")
                kwargs['versions'] = True
                self.delete_folder("", **kwargs)
                kwargs['versions'] = False
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
                except:
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


class CosS3Client(object):

    def __init__(self, conf):
        self._conf = conf
        self._session = requests.session()

    def op_int(self):
        return Interface(conf=self._conf, session=self._session)


if __name__ == "__main__":
    pass
