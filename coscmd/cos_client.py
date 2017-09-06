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
import binascii
import datetime
import pytz
import urllib
from tqdm import tqdm

logger = logging.getLogger(__name__)
fs_coding = sys.getfilesystemencoding()


def to_unicode(s):
    if isinstance(s, unicode):
        return s
    else:
        return s.decode(fs_coding)


def to_printable_str(s):
    if isinstance(s, unicode):
        return s.encode(fs_coding)
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
    return os.path.expanduser('~/.tmp/' + binascii.b2a_hex(base64.encodestring(ori_file))[0:20])


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
    messgae = ""
    code = rt.status_code
    try:
        root = minidom.parseString(rt.content).documentElement
        message = root.getElementsByTagName("Message")[0].childNodes[0].data
    except Exception:
        message = "Not Found"
    return ("error: [code {code}] {message}".format(
                     code=code,
                     message=to_printable_str(message)))


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

    def __init__(self, appid, region, bucket, access_id, access_key, part_size=1, max_thread=5, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._access_id = access_id
        self._access_key = access_key
        self._part_size = min(10, part_size)
        self._max_thread = min(10, max_thread)
        logger.debug("config parameter-> appid: {appid}, region: {region}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
                 appid=appid,
                 region=region,
                 bucket=bucket,
                 part_size=part_size,
                 max_thread=max_thread))

    def uri(self, path=None):
        if path:
            url = u"http://{bucket}-{uid}.cos.{region}.myqcloud.com/{path}".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region,
                path=to_unicode(path)
            )
        else:
            url = u"http://{bucket}-{uid}.cos.{region}.myqcloud.com".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region
            )
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
        url = self._conf.uri(path=cos_path)
        s = requests.Session()
        req = requests.Request('GET',  url)
        prepped = s.prepare_request(req)
        signature = CosS3Auth(self._conf._access_id, self._conf._access_key, timeout).__call__(prepped).headers['Authorization']
        return url + '?sign=' + urllib.quote(signature)

    def list_part(self, cos_path):
        logger.debug("getting uploaded parts")
        NextMarker = ""
        IsTruncated = "true"
        cos_path = to_printable_str(cos_path)
        while IsTruncated == "true":
            url = self._conf.uri(path=cos_path+'?uploadId={UploadId}&upload&max-parts=1000&part-number-marker={nextmarker}'.format(UploadId=self._upload_id, nextmarker=NextMarker))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))

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
                return False
        logger.debug("list parts error")
        return True

    def upload_folder(self, local_path, cos_path):

        local_path = to_unicode(local_path)
        cos_path = to_unicode(cos_path)
        filelist = os.listdir(local_path)
        if cos_path[-1] != '/':
            cos_path += '/'
        if local_path[-1] != '/':
            local_path += '/'
        self._folder_num += 1
        ret_code = True  # True means 0, False means -1
        for filename in filelist:
            filepath = os.path.join(local_path, filename)
            if os.path.isdir(filepath):
                if not self.upload_folder(filepath, cos_path+filename):
                    ret_code = False
            else:
                if self.upload_file(local_path=filepath, cos_path=cos_path+filename) is False:
                    logger.info("upload {file} fail".format(file=to_printable_str(filepath)))
                    self._fail_num += 1
                    ret_code = False
                else:
                    self._file_num += 1
                    logger.debug("upload {file} success".format(file=to_printable_str(filepath)))
        return ret_code

    def upload_file(self, local_path, cos_path):

        def single_upload():
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as File:
                    data = File.read()
            url = self._conf.uri(path=cos_path)
            for j in range(self._retry):
                try:
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data)
                    if rt.status_code == 200:
                        if local_path != '':
                            logger.info("upload {file} with {per}%".format(file=to_printable_str(local_path), per="{0:5.2f}".format(100)))
                        return True
                    else:
                        time.sleep(2**j)
                        logger.warn(response_info(rt))
                        continue
                    if j+1 == self._retry:
                        return False
                except Exception as e:
                    logger.warn("upload file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._md5 = {}
            self._have_finished = 0
            self._have_uploaded = []
            self._upload_id = None
            self._path_md5 = get_md5_filename(local_path, cos_path)
            logger.debug("init with : " + url)
            if os.path.isfile(self._path_md5):
                with open(self._path_md5, 'rb') as f:
                    self._upload_id = f.read()
                if self.list_part(cos_path) is True:
                    logger.info("continue uploading from last breakpoint")
                    return True
            rt = self._session.post(url=url+"?uploads", auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))

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
                logger.debug("upload url: " + str(url))
                for j in range(self._retry):
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._access_id, self._conf._access_key),
                                           data=data)
                    logger.debug("multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx,
                        round=j+1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=rt.text))
                    self._md5[idx] = rt.headers[self._etag][1:-1]
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
            logger.info("uploading {file}".format(file=to_printable_str(local_path)))
            self._pbar = tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024)
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
            logger.info('completing multiupload')
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
                with closing(self._session.post(url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data, stream=True)) as rt:
                    logger.debug("complete status code: {code}".format(code=rt.status_code))
                    logger.debug("complete headers: {headers}".format(headers=rt.headers))
                if rt.status_code == 200:
                    os.remove(self._path_md5)
                    return True
                else:
                    logger.warn(response_info(rt))
                    return False
            except Exception as e:
                return False
            return True

        if local_path == "":
            file_size = 0
        else:
            file_size = os.path.getsize(local_path)
        if file_size < 5*1024*1024:
            for i in range(self._retry):
                if single_upload() is True:
                    return True
            return False
        else:
            for i in range(self._retry):

                rt = init_multiupload()
                if rt:
                    break
            else:
                return False
            logger.debug("Init multipart upload ok")

            for i in range(self._retry):
                rt = multiupload_parts()
                if rt:
                    break
            else:
                return False
            logger.debug("multipart upload ok")
            for i in range(self._retry):
                rt = complete_multiupload()
                if rt:
                    logger.debug("complete multipart upload ok")
                    return True
            logger.warn("complete multipart upload failed")
            return False

    def download_folder(self, cos_path, local_path, _force=False):

        def download_file(_cos_path, _local_path, _force):
            if self.download_file(_cos_path, _local_path, _force) is True:
                logger.info("download {file}".format(file=to_printable_str(_cos_path)))
                self._have_finished += 1
            else:
                logger.info("download {file} fail".format(file=to_printable_str(_cos_path)))
                self._fail_num += 1

        NextMarker = ""
        IsTruncated = "true"
        self._file_num = 0
        self._have_finished = 0
        self._fail_num = 0
        cos_path = to_unicode(cos_path)
        while IsTruncated == "true":
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}'.format(prefix=to_printable_str(cos_path), nextmarker=to_printable_str(NextMarker)))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            if rt.status_code == 200:
                root = minidom.parseString(rt.content).documentElement
                IsTruncated = root.getElementsByTagName("IsTruncated")[0].childNodes[0].data
                if IsTruncated == 'true':
                    NextMarker = root.getElementsByTagName("NextMarker")[0].childNodes[0].data
                fileset = root.getElementsByTagName("Contents")
                for _file in fileset:
                    self._file_num += 1
                    _cos_path = _file.getElementsByTagName("Key")[0].childNodes[0].data
                    _local_path = local_path + _cos_path[len(cos_path):]
                    _cos_path = to_unicode(_cos_path)
                    _local_path = to_unicode(_local_path)
                    download_file(_cos_path, _local_path, _force)
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
        if _force is False and os.path.isfile(local_path) is True:
            logger.warn("The file {file} already exists, please use -f to overwrite the file".format(file=to_printable_str(cos_path)))
            return False
        url = self._conf.uri(path=cos_path)
        logger.debug("download with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), stream=True)
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            if 'Content-Length' in rt.headers:
                content_len = int(rt.headers['Content-Length'])
            else:
                raise IOError("download failed without Content-Length header")
            if rt.status_code == 200:
                self._pbar = tqdm(total=content_len, unit='B', unit_scale=True, unit_divisor=1024)
                file_len = 0
                dir_path = os.path.dirname(local_path)
                if os.path.isdir(dir_path) is False and dir_path != '':
                    try:
                        os.makedirs(dir_path)
                    except Exception as e:
                        logger.warn(str(e))
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
                self._pbar.close()
                return True
            else:
                logger.warn(response_info(rt))
                return False
        except Exception as e:
            logger.warn(str(e))
        return False

    def delete_folder(self, cos_path):

        cos_path = to_unicode(cos_path)
        # make sure
        if query_yes_no("WARN: you are deleting all files under cos_path '{cos_path}', please make sure".format(cos_path=to_printable_str(cos_path))) is False:
            return False
        self._have_finished = 0
        self._file_num = 0
        NextMarker = ""
        IsTruncated = "true"
        while IsTruncated == "true":
            data_xml = ""
            file_list = []
            url = self._conf.uri(path='?max-keys=1000&marker={nextmarker}&prefix={prefix}'.format(nextmarker=to_printable_str(NextMarker), prefix=to_printable_str(cos_path)))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
                rt = self._session.post(url=url_file, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data_xml, headers=http_header)
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
        if self._file_num == 0:
            logger.info("The directory does not exist")
            return False
        logger.info("{files} files successful, {fail_files} files failed"
                    .format(files=self._have_finished, fail_files=self._fail_num))
        if self._file_num == self._have_finished:
            return True
        else:
            return False

    def delete_file(self, cos_path):
        url = self._conf.uri(path=cos_path)
        logger.info("delete with : " + url)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
        table = PrettyTable(["Path", "Size/Type", "Time"])
        table.align = "l"
        table.align['Size/Type'] = 'r'
        table.padding_width = 3
        table.header = False
        self._file_num = 0
        cos_path = to_printable_str(cos_path)
        while IsTruncated == "true":
            url = self._conf.uri(path='?prefix={prefix}&marker={nextmarker}{delimiter}'
                                 .format(prefix=to_printable_str(cos_path), nextmarker=to_printable_str(NextMarker), delimiter=Delimiter))
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
                    if _human is True:
                        _size = change_to_human(_size)
                    _path = _file.getElementsByTagName("Key")[0].childNodes[0].data
                    table.add_row([_path, _size, _time])
                    if self._file_num == _num:
                        break
                if self._file_num == _num:
                    break
            else:
                logger.warn(response_info(rt))
                return False
        print table
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
            rt = self._session.head(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
                    rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
                    logger.debug("get resp, status code: {code}, headers: {headers}".format(
                         code=rt.status_code,
                         headers=rt.headers))
                    if rt.status_code == 200:
                        root = minidom.parseString(rt.content).documentElement
                        grants = root.getElementsByTagName("Grant")
                        for grant in grants:
                            table.add_row(['ACL', ("%s: %s" %
                                                   (grant.getElementsByTagName("ID")[0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))])
                    else:
                        logger.warn(response_info(rt))
                except Exception as e:
                    logger.warn(str(e))
                    return False
                print table
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
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data)
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
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                logger.info("%s => %s" % (grant.getElementsByTagName("ID")[0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))
            if rt.status_code == 200:
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
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))

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
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
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
            rt = self._session.put(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data)
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
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("get resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            root = minidom.parseString(rt.content).documentElement
            grants = root.getElementsByTagName("Grant")
            for grant in grants:
                logger.info("%s => %s" % (grant.getElementsByTagName("ID")[0].childNodes[0].data, grant.getElementsByTagName("Permission")[0].childNodes[0].data))
            if rt.status_code == 200:
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
