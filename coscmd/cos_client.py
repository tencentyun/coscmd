# -*- coding=utf-8
from cos_auth import CosS3Auth
from cos_threadpool import SimpleThreadPool
from prettytable import PrettyTable
from os import path
from contextlib import closing
from xml.dom import minidom
from hashlib import md5
import time
import requests
import logging
import sys
import os
import base64
import datetime
import pytz
import urllib
from tqdm import tqdm
from wsgiref.handlers import format_date_time
logger = logging.getLogger(__name__)
fs_coding = sys.getfilesystemencoding()


def to_unicode(s):
    if isinstance(s, unicode):
        return s
    else:
        return s.decode(fs_coding)


def to_printable_str(s):
    if isinstance(s, unicode):
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
    ori_file = os.path.abspath(os.path.dirname(local_path)) + "!!!" + str(os.path.getsize(local_path)) + "!!!" + cos_path
    m = md5()
    m.update(to_printable_str(ori_file))
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
        choice = raw_input().lower()
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
        request_id = root.getElementsByTagName("RequestId")[0].childNodes[0].data
    except Exception:
        message = "Not Found"
    return ('''Error: [code {code}] {message}
RequestId: {request_id}'''.format(
                     code=code,
                     message=to_printable_str(message),
                     request_id=to_printable_str(request_id)))


def utc_to_local(utc_time_str, utc_format='%Y-%m-%dT%H:%M:%S.000Z'):
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


class CosConfig(object):

    def __init__(self, appid, region, bucket, secret_id, secret_key, part_size=1, max_thread=5, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._secret_id = secret_id
        self._secret_key = secret_key
        self._part_size = min(10, part_size)
        self._max_thread = min(10, max_thread)
        self._schema = "https"
        logger.debug("config parameter-> appid: {appid}, region: {region}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
                 appid=appid,
                 region=region,
                 bucket=bucket,
                 part_size=part_size,
                 max_thread=max_thread))

    def uri(self, path=None):
        if path:
            url = u"{schema}://{bucket}-{uid}.cos.{region}.myqcloud.com/{path}".format(
                schema=self._schema,
                bucket=self._bucket,
                uid=self._appid,
                region=self._region,
                path=to_unicode(path)
            )
        else:
            url = u"{schema}://{bucket}-{uid}.cos.{region}.myqcloud.com".format(
                schema=self._schema,
                bucket=self._bucket,
                uid=self._appid,
                region=self._region
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
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

    def sign_url(self, cos_path, timeout=10000):
        cos_path = to_printable_str(cos_path)
        url = self._conf.uri(path=urllib.quote(to_printable_str(cos_path)))
        s = requests.Session()
        req = requests.Request('GET',  url)
        prepped = s.prepare_request(req)
        signature = CosS3Auth(self._conf._secret_id, self._conf._secret_key, timeout).__call__(prepped).headers['Authorization']

        return to_printable_str(url + '?sign=' + urllib.quote(signature))

    def list_part(self, cos_path):
        logger.debug("getting uploaded parts")
        NextMarker = ""
        IsTruncated = "true"
        cos_path = to_printable_str(cos_path)
        try:
            while IsTruncated == "true":
                url = self._conf.uri(path=cos_path+'?uploadId={UploadId}&upload&max-parts=1000&part-number-marker={nextmarker}'.format(UploadId=self._upload_id, nextmarker=NextMarker))
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
    
                if rt.status_code == 200:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName("NextPartNumberMarker")[0].childNodes[0].data
                    logger.debug("list resp, status code: {code}, headers: {headers}, text: {text}".format(
                         code=rt.status_code,
                         headers=rt.headers,
                         text=to_printable_str(rt.text)))
                    contentset = root.getElementsByTagName("Part")
                    for content in contentset:
                        ID = content.getElementsByTagName("PartNumber")[0].childNodes[0].data
                        self._have_uploaded.append(ID)
                        self._md5[int(ID)] = content.getElementsByTagName(self._etag)[0].childNodes[0].data[1:-1]
                else:
                    logger.debug(response_info(rt))
                    logger.debug("list parts error")
                    return False
        except Exception:
            logger.debug("list parts error")
            return False
        return True

    def upload_folder(self, local_path, cos_path, _type='Standard', _encryption=''):

        local_path = to_unicode(local_path)
        cos_path = to_unicode(cos_path)
        filelist = os.listdir(local_path)
        if cos_path.endswith('/') is False:
            cos_path += "/"
        if local_path.endswith('/') is False:
            local_path += '/'
        cos_path = cos_path.lstrip('/')
        self._type = _type
        self._folder_num += 1
        ret_code = True  # True means 0, False means -1
        for filename in filelist:
            filepath = os.path.join(local_path, filename)
            if os.path.isdir(filepath):
                if not self.upload_folder(filepath, cos_path+filename, _type, _encryption):
                    ret_code = False
            else:
                if self.upload_file(local_path=filepath, cos_path=cos_path+filename, _type=_type, _encryption=_encryption) is False:
                    logger.info("upload {file} fail".format(file=to_printable_str(filepath)))
                    self._fail_num += 1
                    ret_code = False
                else:
                    self._file_num += 1
                    logger.debug("upload {file} success".format(file=to_printable_str(filepath)))
        return ret_code

    def upload_file(self, local_path, cos_path, _type='Standard', _encryption=''):

        def single_upload():
            self._type = _type
            self._encryption = _encryption
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as File:
                    data = File.read()
            url = self._conf.uri(path=cos_path)
            for j in range(self._retry):
                try:
                    http_header = dict()
                    http_header['x-cos-storage-class'] = self._type
                    if _encryption != '':
                        http_header['x-cos-server-side-encryption'] = self._encryption
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data, headers=http_header)
                    if rt.status_code == 200:
                        if local_path != '':
                            logger.info("upload {local_path}   =>   cos://{bucket}/{cos_path}  [100%]".format(
                                                                    bucket=self._conf._bucket,
                                                                    local_path=to_printable_str(local_path),
                                                                    cos_path=to_printable_str(cos_path)))
                        return True
                    else:
                        time.sleep(2**j)
                        logger.warn(response_info(rt))
                        continue
                    if j+1 == self._retry:
                        return False
                except Exception:
                    logger.warn("upload file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._md5 = {}
            self._have_finished = 0
            self._have_uploaded = []
            self._upload_id = None
            self._type = _type
            self._path_md5 = get_md5_filename(local_path, cos_path)
            self._encryption = _encryption
            logger.debug("init with : " + url)
            if os.path.isfile(self._path_md5):
                with open(self._path_md5, 'rb') as f:
                    self._upload_id = f.read()
                if self.list_part(cos_path) is True:
                    logger.info("continue uploading from last breakpoint")
                    return True
            http_header = dict()
            http_header['x-cos-storage-class'] = self._type
            if _encryption != '':
                http_header['x-cos-server-side-encryption'] = self._encryption
            rt = self._session.post(url=url+"?uploads", auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), headers=http_header)
            logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=to_printable_str(rt.text)))

            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                self._upload_id = root.getElementsByTagName("UploadId")[0].childNodes[0].data
                if os.path.isdir(os.path.expanduser("~/.tmp")) is False:
                    os.makedirs(os.path.expanduser("~/.tmp"))
                with open(self._path_md5, 'wb') as f:
                    f.write(self._upload_id)
                return True
            else:
                logger.warn(response_info(rt))
                return False
            return True

        def multiupload_parts():

            def multiupload_parts_data(local_path, offset, length, parts_size, idx):
                with open(local_path, 'rb') as File:
                    File.seek(offset, 0)
                    data = File.read(length)
                url = self._conf.uri(path=cos_path)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx, uploadid=self._upload_id)
                for j in range(self._retry):
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key),
                                           data=data)
                    logger.debug("multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx,
                        round=j+1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=to_printable_str(rt.text)))
                    self._md5[idx] = rt.headers[self._etag][1:-1]
                    logger.debug("local md5: {key}".format(key=self._md5[idx]))
                    logger.debug("cos md5: {key}".format(key=md5(data).hexdigest()))
                    if rt.status_code == 200:
                        if(self._md5[idx] == md5(data).hexdigest()):
                            self._have_finished += 1
                            self._pbar.update(length)
                            break
                        else:
                            logger.warn("md5 verification is inconsistent")
                            continue
                    else:
                        logger.warn(response_info(rt))
                        time.sleep(2**j)
                        continue
                    if j+1 == self._retry:
                        logger.warn("upload part failed: part{part}, round{round}, code: {code}".format(part=idx, round=j+1, code=rt.status_code))
                        return False
                return True

            offset = 0
            file_size = path.getsize(local_path)
            logger.debug("file size: " + str(file_size))
            chunk_size = 1024 * 1024 * self._conf._part_size
            while file_size / chunk_size > 8000:
                chunk_size = chunk_size * 10
            parts_num = file_size / chunk_size
            last_size = file_size - parts_num * chunk_size
            self._have_finished = len(self._have_uploaded)
            if last_size != 0:
                parts_num += 1
            _max_thread = min(self._conf._max_thread, parts_num - self._have_finished)
            pool = SimpleThreadPool(_max_thread)

            logger.debug("chunk_size: " + str(chunk_size))
            logger.debug('upload file concurrently')
            logger.info("upload {local_path}   =>   cos://{bucket}/{cos_path}".format(
                                                    bucket=self._conf._bucket,
                                                    local_path=to_printable_str(local_path),
                                                    cos_path=to_printable_str(cos_path)))
            self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
            if chunk_size >= file_size:
                pool.add_task(multiupload_parts_data, local_path, offset, file_size, 1, 0)
            else:
                for i in range(parts_num):
                    if(str(i+1) in self._have_uploaded):
                        offset += chunk_size
                        self._pbar.update(chunk_size)
                        continue
                    if i+1 == parts_num:
                        pool.add_task(multiupload_parts_data, local_path, offset, file_size-offset, parts_num, i+1)
                    else:
                        pool.add_task(multiupload_parts_data, local_path, offset, chunk_size, parts_num, i+1)
                        offset += chunk_size
            pool.wait_completion()
            result = pool.get_result()
            self._pbar.close()
            if result['success_all']:
                return True
            else:
                return False

        def complete_multiupload():
            print('completing multiupload, please wait')
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
                url = self._conf.uri(path=cos_path)+"?uploadId={uploadid}".format(uploadid=self._upload_id)
                logger.debug('complete url: ' + url)
                logger.debug("complete data: " + data)
            try:
                with closing(self._session.post(url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data, stream=True)) as rt:
                    logger.debug("complete status code: {code}".format(code=rt.status_code))
                    logger.debug("complete headers: {headers}".format(headers=rt.headers))
                if rt.status_code == 200:
                    os.remove(self._path_md5)
                    return True
                else:
                    logger.warn(response_info(rt))
                    return False
            except Exception:
                return False
            return True
        if local_path == "":
            file_size = 0
        else:
            file_size = os.path.getsize(local_path)
        if file_size < 5*1024*1024:
            for _ in range(self._retry):
                if single_upload() is True:
                    return True
            return False
        else:
            for _ in range(self._retry):
                rt = init_multiupload()
                if rt:
                    break
            else:
                return False
            logger.debug("Init multipart upload ok")

            rt = multiupload_parts()
            if rt is False:
                return False
            logger.debug("multipart upload ok")
            for _ in range(self._retry):
                rt = complete_multiupload()
                if rt:
                    logger.debug("complete multipart upload ok")
                    return True
            logger.warn("complete multipart upload failed")
            return False

    def copy_file(self, source_path, cos_path, _type='Standard'):

        def single_copy():
            self._type = _type
            url = self._conf.uri(path=cos_path)
            for j in range(self._retry):
                try:
                    http_header = dict()
                    http_header['x-cos-storage-class'] = self._type
                    http_header['x-cos-copy-source'] = source_path
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), headers=http_header)
                    if rt.status_code == 200:
                        logger.info("copy {source_path}   =>   cos://{bucket}/{cos_path}  [100%]".format(
                                                                    bucket=self._conf._bucket,
                                                                    source_path=to_printable_str(source_path),
                                                                    cos_path=to_printable_str(cos_path)))
                        return True
                    else:
                        time.sleep(2**j)
                        logger.warn(response_info(rt))
                        continue
                    if j+1 == self._retry:
                        return False
                except Exception:
                    logger.warn("copy file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._md5 = {}
            self._have_finished = 0
            self._upload_id = None
            self._type = _type
            http_header = dict()
            http_header['x-cos-storage-class'] = self._type
            rt = self._session.post(url=url+"?uploads", auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), headers=http_header)
            logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=to_printable_str(rt.text)))

            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                self._upload_id = root.getElementsByTagName("UploadId")[0].childNodes[0].data
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
                    logger.warn("Source path format error")
                return source_bucket, source_appid, source_region, source_cospath

            def copy_parts_data(local_path, offset, length, parts_size, idx):
                url = self._conf.uri(path=cos_path)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx, uploadid=self._upload_id)
                http_header = dict()
                http_header['x-cos-copy-source'] = source_path
                http_header['x-cos-copy-source-range'] = "bytes="+str(offset)+"-"+str(offset+length-1)
                for j in range(self._retry):
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key),
                                           headers=http_header)
                    logger.debug("copy part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx,
                        round=j+1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=to_printable_str(rt.text)))
                    root = minidom.parseString(rt.content).documentElement
                    self._md5[idx] = root.getElementsByTagName("ETag")[0].childNodes[0].data
                    if rt.status_code == 200:
                        self._have_finished += 1
                        self._pbar.update(length)
                        break
                    else:
                        logger.warn(response_info(rt))
                        time.sleep(2**j)
                        continue
                    if j+1 == self._retry:
                        logger.warn("upload part failed: part{part}, round{round}, code: {code}".format(part=idx, round=j+1, code=rt.status_code))
                        return False
                return True

            offset = 0
            logger.debug("file size: " + str(file_size))
            chunk_size = 1024 * 1024 * 5
            while file_size / chunk_size > 8000:
                chunk_size = chunk_size * 10
            parts_num = file_size / chunk_size
            last_size = file_size - parts_num * chunk_size
            if last_size != 0:
                parts_num += 1
            _max_thread = min(self._conf._max_thread, parts_num)
            pool = SimpleThreadPool(_max_thread)

            logger.debug("chunk_size: " + str(chunk_size))
            logger.debug('copy file concurrently')
            logger.info("copy {source_path}   =>   cos://{bucket}/{cos_path}".format(
                                                    bucket=self._conf._bucket,
                                                    source_path=to_printable_str(source_path),
                                                    cos_path=to_printable_str(cos_path)))
            self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
            if chunk_size >= file_size:
                pool.add_task(copy_parts_data, source_path, offset, file_size, 1, 0)
            else:
                for i in range(parts_num):
                    if i+1 == parts_num:
                        pool.add_task(copy_parts_data, source_path, offset, file_size-offset, parts_num, i+1)
                    else:
                        pool.add_task(copy_parts_data, source_path, offset, chunk_size, parts_num, i+1)
                        offset += chunk_size
            pool.wait_completion()
            result = pool.get_result()
            self._pbar.close()
            if result['success_all']:
                return True
            else:
                return False

        def complete_multiupload():
            print('completing multicopy, please wait')
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
                url = self._conf.uri(path=cos_path)+"?uploadId={uploadid}".format(uploadid=self._upload_id)
                logger.debug('complete url: ' + url)
                logger.debug("complete data: " + data)
            try:
                with closing(self._session.post(url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data, stream=True)) as rt:
                    logger.debug("complete status code: {code}".format(code=rt.status_code))
                    logger.debug("complete headers: {headers}".format(headers=rt.headers))
                if rt.status_code == 200:
                    return True
                else:
                    logger.warn(response_info(rt))
                    return False
            except Exception:
                return False
            return True

        rt = self._session.head(url="http://"+source_path, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
        if rt.status_code != 200:
            logger.warn("Replication sources do not exist")
            return False
        file_size = int(rt.headers['Content-Length'])
        if file_size < 10*1024*1024:
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
            logger.debug("Init multipart copy ok")

            rt = copy_parts(file_size=file_size)
            if rt is False:
                return False
            logger.debug("multipart copy ok")
            for _ in range(self._retry):
                rt = complete_multiupload()
                if rt:
                    logger.debug("complete multipart copy ok")
                    return True
            logger.warn("complete multipart copy failed")
            return False

    def download_folder(self, cos_path, local_path, _force=False):

        def download_file(_cos_path, _local_path, _force):
            if self.download_file(_cos_path, _local_path, _force) is True:
                self._have_finished += 1
            else:
                self._fail_num += 1

        if cos_path.endswith('/') is False:
            cos_path += '/'
        if local_path.endswith('/') is False:
            local_path += '/'
        cos_path = cos_path.lstrip('/')
        NextMarker = ""
        IsTruncated = "true"
        self._file_num = 0
        self._have_finished = 0
        self._fail_num = 0
        cos_path = to_unicode(cos_path)
        while IsTruncated == "true":
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}'
                                 .format(prefix=urllib.quote(to_printable_str(cos_path)), nextmarker=urllib.quote(to_printable_str(NextMarker))))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data
                fileset = root.getElementsByTagName("Contents")
                for _file in fileset:
                    _cos_path = _file.getElementsByTagName("Key")[0].childNodes[0].data
                    _local_path = local_path + _cos_path[len(cos_path):]
                    _cos_path = to_unicode(_cos_path)
                    _local_path = to_unicode(_local_path)
                    if _cos_path.endswith('/'):
                        continue
                    download_file(_cos_path, _local_path, _force)
                    self._file_num += 1
            else:
                logger.warn(response_info(rt))
                return False
        if self._file_num == 0:
            logger.info("The directory does not exist")
            return False
        logger.info("{files} files successful, {fail_files} files failed"
                    .format(files=self._have_finished, fail_files=self._fail_num))
        if self._file_num == self._have_finished:
            return True
        else:
            return False

    def download_file(self, cos_path, local_path, _force=False):
        cos_path = cos_path.lstrip('/')
        if _force is False and os.path.isfile(local_path) is True:
            logger.warn("The file {file} already exists, please use -f to overwrite the file".format(file=to_printable_str(cos_path)))
            return False
        # logger.info("download {file}".format(file=to_printable_str(cos_path)))
        url = self._conf.uri(path=cos_path)
        logger.info("download cos://{bucket}/{cos_path}   =>   {local_path}".format(
                                                        bucket=self._conf._bucket,
                                                        local_path=to_printable_str(local_path),
                                                        cos_path=to_printable_str(cos_path)))
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), stream=True)
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            if 'Content-Length' in rt.headers:
                content_len = int(rt.headers['Content-Length'])
            else:
                raise IOError("download failed without Content-Length header")
            if rt.status_code == 200:
                with tqdm(total=content_len, unit='B', unit_scale=True) as self._pbar:
                    file_len = 0
                    dir_path = os.path.dirname(local_path)
                    if os.path.isdir(dir_path) is False and dir_path != '':
                        try:
                            print dir_path
                            os.makedirs(dir_path)
                        except Exception as e:
                            logger.warn("unable to create the corresponding folder")
                            return False
                    with open(local_path, 'wb') as f:
                        for chunk in rt.iter_content(chunk_size=1024):
                            if chunk:
                                self._pbar.update(len(chunk))
                                file_len += len(chunk)
                                f.write(chunk)
                        f.flush()
                    if file_len != content_len:
                        raise IOError("download failed with incomplete file")
                    return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def delete_folder(self, cos_path, _force=False):

        cos_path = to_unicode(cos_path)
        if cos_path == "/":
            cos_path = ""
        # make sure
        if _force is False:
            if query_yes_no("WARN: you are deleting all files under cos_path '{cos_path}', please make sure".format(cos_path=to_printable_str(cos_path))) is False:
                return False
        self._have_finished = 0
        self._fail_num = 0
        self._file_num = 0
        NextMarker = ""
        IsTruncated = "true"
        while IsTruncated == "true":
            data_xml = ""
            file_list = []
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}'
                                 .format(prefix=urllib.quote(to_printable_str(cos_path)), nextmarker=urllib.quote(to_printable_str(NextMarker))))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            if rt.status_code == 200:
                try:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data
                except Exception as e:
                    logger.warn(str(e))
                logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                     code=rt.status_code,
                     headers=rt.headers,
                     text=to_printable_str(rt.text)))
                contentset = root.getElementsByTagName("Key")
                for content in contentset:
                    self._file_num += 1
                    file_name = to_unicode(content.childNodes[0].data)
                    file_list.append(file_name)
                    data_xml = data_xml + '''
    <Object>
        <Key>{Key}</Key>
    </Object>'''.format(Key=to_printable_str(file_name))
                data_xml = '''
<Delete>
    <Quiet>true</Quiet>'''+data_xml+'''
</Delete>'''
                http_header = dict()
                md5_ETag = md5()
                md5_ETag.update(data_xml)
                md5_ETag = md5_ETag.digest()
                http_header['Content-MD5'] = base64.b64encode(md5_ETag)
                url_file = self._conf.uri(path="?delete")
                rt = self._session.post(url=url_file, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data_xml, headers=http_header)
                if rt.status_code == 204 or rt.status_code == 200:
                    for file_name in file_list:
                        logger.info("delete {file}".format(file=to_printable_str(file_name)))
                    self._have_finished += len(file_list)
                else:
                    for file_name in file_list:
                        logger.info("delete {file} fail".format(file=to_printable_str(file_name)))
                    self._fail_num += len(file_list)
            else:
                logger.warn(response_info(rt))
                logger.debug("get folder error")
                return False
        # Clipping
        logger.info("Delete the remaining files again")
        IsTruncated = "true"
        while IsTruncated == "true":
            data_xml = ""
            file_list = []
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}'
                                 .format(prefix=urllib.quote(to_printable_str(cos_path)), nextmarker=urllib.quote(to_printable_str(NextMarker))))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            if rt.status_code == 200:
                try:
                    root = minidom.parseString(rt.content).documentElement
                    IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                    if IsTruncated == 'true':
                        NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data
                except Exception as e:
                    logger.warn(str(e))
                logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                     code=rt.status_code,
                     headers=rt.headers,
                     text=to_printable_str(rt.text)))
                contentset = root.getElementsByTagName("Key")
                for content in contentset:
                    file_name = to_unicode(content.childNodes[0].data)
                    if self.delete_file(file_name, True) is True:
                        logger.info("delete {file}".format(file=to_printable_str(file_name)))
                    else:
                        logger.info("delete {file} fail".format(file=to_printable_str(file_name)))
            else:
                logger.warn(response_info(rt))
                logger.debug("get folder error")
                return False
        if self._file_num == 0:
            logger.info("The directory does not exist")
            return False

        logger.info("{files} files successful, {fail_files} files failed"
                    .format(files=self._have_finished, fail_files=self._fail_num))
        if self._file_num == self._have_finished:
            return True
        else:
            return False

    def delete_file(self, cos_path, _force=False):
        if _force is False:
            if query_yes_no("WARN: you are deleting the file in the '{cos_path}' cos_path, please make sure".format(cos_path=to_printable_str(cos_path))) is False:
                return False
        url = self._conf.uri(path=cos_path)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            if rt.status_code == 204 or rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def list_objects(self, cos_path, _recursive=False, _all=False, _num=100, _human=False):
        NextMarker = ""
        IsTruncated = "true"
        Delimiter = "&delimiter=/"
        if _recursive is True:
            Delimiter = ""
        if _all is True:
            _num = -1
        self._file_num = 0
        self._total_size = 0
        cos_path = to_printable_str(cos_path)
        while IsTruncated == "true":
            table = PrettyTable(["Path", "Size/Type", "Time"])
            table.align = "l"
            table.align['Size/Type'] = 'r'
            table.padding_width = 3
            table.header = False
            table.border = False
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}{delimiter}'
                                 .format(prefix=urllib.quote(to_printable_str(cos_path)), nextmarker=urllib.quote(to_printable_str(NextMarker)), delimiter=Delimiter))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data

                logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                     code=rt.status_code,
                     headers=rt.headers,
                     text=to_printable_str(rt.text)))
                folderset = root.getElementsByTagName("CommonPrefixes")
                for _folder in folderset:
                    _time = ""
                    _type = "DIR"
                    _path = _folder.getElementsByTagName("Prefix")[0].childNodes[0].data
                    table.add_row([_path, _type, _time])
                fileset = root.getElementsByTagName("Contents")
                for _file in fileset:
                    self._file_num += 1
                    _time = _file.getElementsByTagName("LastModified")[0].childNodes[0].data
                    _time = time.localtime(utc_to_local(_time))
                    _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                    _size = _file.getElementsByTagName("Size")[0].childNodes[0].data
                    self._total_size += int(_size)
                    if _human is True:
                        _size = change_to_human(_size)
                    _path = _file.getElementsByTagName("Key")[0].childNodes[0].data
                    table.add_row([_path, _size, _time])
                    if self._file_num == _num:
                        break
                try:
                    print unicode(table)
                except Exception:
                    print table
                if self._file_num == _num:
                    break
            else:
                logger.warn(response_info(rt))
                return False
        if _human:
            self._total_size = change_to_human(str(self._total_size))
        else:
            self._total_size = str(self._total_size)
        if _recursive:
            logger.info(" Files num: {file_num}".format(file_num=str(self._file_num)))
            logger.info(" Files size: {file_size}".format(file_size=self._total_size))
        if _all is False and self._file_num == _num:
            logger.info("Has listed the first {num}, use \'-a\' option to list all please".format(num=self._file_num))
        return True

    def info_object(self, cos_path, _human=False):
        table = PrettyTable([cos_path, ""])
        table.align = "l"
        table.padding_width = 3
        url = self._conf.uri(path=cos_path)
        logger.info("info with : " + url)
        cos_path = to_printable_str(cos_path)
        try:
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("info resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            if rt.status_code == 200:
                _size = rt.headers['Content-Length']
                if _human is True:
                    _size = change_to_human(_size)
                _time = time.localtime(utc_to_local(rt.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S GMT'))
                _time = time.strftime("%Y-%m-%d %H:%M:%S", _time)
                table.add_row(['File size', _size])
                table.add_row(['Last mod', _time])
                url = self._conf.uri(cos_path+"?acl")
                try:
                    rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
                    logger.debug("get resp, status code: {code}, headers: {headers}".format(
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
                                table.add_row(['ACL', ("%s: %s" % ('anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                    else:
                        logger.warn(response_info(rt))
                except Exception as e:
                    logger.warn(str(e))
                    return False
                try:
                    print unicode(table)
                except Exception as e:
                    print table
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return False

    def mget(self, cos_path, local_path, _force=False, _num=10):

        def get_parts_data(local_path, offset, length, parts_size, idx):
            try:
                local_path = local_path + "_" + str(idx)
                http_header = {}
                http_header['Range'] = 'bytes=' + str(offset) + "-" + str(offset+length-1)
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), headers=http_header, stream=True)
                logger.debug("get resp, status code: {code}, headers: {headers}".format(
                     code=rt.status_code,
                     headers=rt.headers))
                if 'Content-Length' in rt.headers:
                    content_len = int(rt.headers['Content-Length'])
                else:
                    raise IOError("download failed without Content-Length header")
                if rt.status_code in [206, 200]:
                    file_len = 0
                    dir_path = os.path.dirname(local_path)
                    if os.path.isdir(dir_path) is False and dir_path != '':
                        try:
                            os.makedirs(dir_path)
                        except Exception as e:
                            logger.warn(str(e))
                            return False
                    with open(local_path, 'wb') as f:
                        for chunk in rt.iter_content(chunk_size=1024*1024):
                            if chunk:
                                file_len += len(chunk)
                                f.write(chunk)
                                self._pbar.update(len(chunk))
                        f.flush()
                    if file_len != content_len:
                        raise IOError("download failed with incomplete file")
                    return True
                else:
                    logger.warn(response_info(rt))
                    return False
            except Exception as e:
                logger.warn(str(e))
            return False

        if _force is False and os.path.isfile(local_path) is True:
            logger.warn("The file {file} already exists, please use -f to overwrite the file".format(file=to_printable_str(cos_path)))
            return False
        url = self._conf.uri(path=cos_path)
        logger.info("info with : " + url)
        cos_path = to_printable_str(cos_path)
        try:
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("info resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            if rt.status_code == 200:
                file_size = int(rt.headers['Content-Length'])
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        # mget
        url = self._conf.uri(path=cos_path)
        logger.debug("mget with : " + url)
        offset = 0
        logger.debug("file size: " + str(file_size))

        parts_num = _num
        chunk_size = file_size / parts_num
        last_size = file_size - parts_num * chunk_size
        self._have_finished = 0
        if last_size != 0:
            parts_num += 1
        _max_thread = min(self._conf._max_thread, parts_num - self._have_finished)
        pool = SimpleThreadPool(_max_thread)

        logger.debug("chunk_size: " + str(chunk_size))
        logger.debug('download file concurrently')
        logger.info("downloading {file}".format(file=to_printable_str(local_path)))
        self._pbar = tqdm(total=file_size, unit='B', unit_scale=True)
        for i in range(parts_num):
            if i+1 == parts_num:
                pool.add_task(get_parts_data, local_path, offset, file_size-offset, parts_num, i+1)
            else:
                pool.add_task(get_parts_data, local_path, offset, chunk_size, parts_num, i+1)
                offset += chunk_size
        pool.wait_completion()
        result = pool.get_result()
        self._pbar.close()
        # complete
        logger.info('completing mget')
        if result['success_all'] is False:
            return False
        with open(local_path, 'wb') as f:
            for i in range(parts_num):
                idx = i + 1
                file_name = local_path + "_" + str(idx)
                length = 1024*1024
                offset = 0
                with open(file_name, 'rb') as File:
                    while (offset < file_size):
                        File.seek(offset, 0)
                        data = File.read(length)
                        f.write(data)
                        offset += length
                os.remove(file_name)
            f.flush()
        return True

    def restore_object(self, cos_path, _day, _tier):
        cos_path = to_printable_str(cos_path)
        url = self._conf.uri(path=cos_path+"?restore")
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
            rt = self._session.post(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data_xml, headers=http_header)
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
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
        url = self._conf.uri(cos_path+"?acl")
        logger.info("put with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
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
                if subid != "anyone":
                    subid = "uin/"+subid
                    rootid = "uin/"+rootid
                grants += '''
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="{accounttype}">
                <ID>qcs::cam::{rootid}:{subid}</ID>
            </Grantee>
            <Permission>{permissiontype}</Permission>
        </Grant>'''.format(rootid=rootid, subid=subid, accounttype=accounttype, permissiontype=Type)

            data = '''<AccessControlPolicy>
    <Owner>
        <ID>{id}</ID>
    </Owner>
    <AccessControlList>'''.format(id=owner_id)+grants+'''
    </AccessControlList>
</AccessControlPolicy>
'''

            logger.debug(data)
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data)
            logger.debug("put resp, status code: {code}, headers: {headers}".format(
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
        url = self._conf.uri(cos_path+"?acl")
        logger.info("get with : " + url)
        table = PrettyTable([cos_path, ""])
        table.align = "l"
        table.padding_width = 3
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                try:
                    table.add_row(['ACL', ("%s: %s" % (grant.getElementsByTagName("ID")[0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                except Exception:
                    table.add_row(['ACL', ("%s: %s" % ('anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
            if rt.status_code == 200:
                try:
                    print unicode(table)
                except Exception as e:
                    print table
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
        logger.debug("create bucket with : " + url)
        try:
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("put resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))
            if rt.status_code == 200:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return True

    def delete_bucket(self):
        url = self._conf.uri(path='')
        self._have_finished = 0
        logger.debug("delete bucket with : " + url)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("delete resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))
            if rt.status_code == 204:
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
            return False
        return True

    def get_bucket(self, max_keys=10):
        NextMarker = ""
        IsTruncated = "true"
        pagecount = 0
        filecount = 0
        sizecount = 0
        while IsTruncated == "true":
            pagecount += 1
            logger.info("get bucket with page {page}".format(page=pagecount))
            url = self._conf.uri(path='?max-keys=1000&marker={nextmarker}'.format(nextmarker=NextMarker))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))

            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data

                logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                     code=rt.status_code,
                     headers=rt.headers,
                     text=to_printable_str(rt.text)))
                contentset = root.getElementsByTagName("Contents")
                for content in contentset:
                    filecount += 1
                    sizecount += int(content.getElementsByTagName("Size")[0].childNodes[0].data)
                    print to_printable_str(content.toxml())
                    if filecount == max_keys:
                        break
            else:
                logger.warn(response_info(rt))
                return False

        logger.info("filecount: %d" % filecount)
        logger.info("sizecount: %d" % sizecount)
        logger.debug("get bucket success")
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
        logger.info("put with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
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
                if subid != "anyone":
                    subid = "uin/"+subid
                    rootid = "uin/"+rootid
                grants += '''
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="{accounttype}">
                <ID>qcs::cam::{rootid}:{subid}</ID>
            </Grantee>
            <Permission>{permissiontype}</Permission>
        </Grant>'''.format(rootid=rootid, subid=subid, accounttype=accounttype, permissiontype=Type)

            data = '''<AccessControlPolicy>
    <Owner>
        <ID>{id}</ID>
    </Owner>
    <AccessControlList>'''.format(id=owner_id)+grants+'''
    </AccessControlList>
</AccessControlPolicy>
'''

            logger.debug(data)
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key), data=data)
            logger.debug("put resp, status code: {code}, headers: {headers}".format(
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
        logger.info("get with : " + url)
        table = PrettyTable([self._conf._bucket, ""])
        table.align = "l"
        table.padding_width = 3
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._secret_id, self._conf._secret_key))
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                try:
                    table.add_row(['ACL', ("%s: %s" % (grant.getElementsByTagName("ID")[0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                except Exception:
                    table.add_row(['ACL', ("%s: %s" % ('anyone', grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
            if rt.status_code == 200:
                try:
                    print unicode(table)
                except Exception as e:
                    print table
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
