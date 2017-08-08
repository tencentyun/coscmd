# -*- coding=utf-8
from cos_auth import CosS3Auth
from cos_threadpool import SimpleThreadPool
import time
import requests
from os import path
from contextlib import closing
from xml.dom import minidom
import logging
import sys
import os
import base64

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


def view_bar(num, total):
    ret = 1.0*num / total
    ag = ret * 100
    ab = "\r [%-50s]%.2f%%" % ('='*int(ret*50), ag, )
    sys.stdout.write(ab)
    sys.stdout.flush()


def getTagText(root, tag):
    node = root.getElementsByTagName(tag)[0]
    rc = ""
    for node in node.childNodes:
        if node.nodeType in (node.TEXT_NODE, node.CDATA_SECTION_NODE):
            rc = rc + node.data


def get_md5_filename(local_path, cos_path):
    ori_file = os.path.abspath(os.path.dirname(local_path)) + "!!!" + str(os.path.getsize(local_path)) + "!!!" + cos_path
    return os.path.expanduser('~/.tmp/' + base64.encodestring(ori_file)[0:10])


def query_yes_no(question, default=None):
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


def response_info(info, rt):
    return ("{info}, status code: {code}, headers: {headers}, text: {text}".format(
                     info=info,
                     code=rt.status_code,
                     headers=rt.headers,
                     text=to_printable_str(rt.text)))


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
            url = u"http://{bucket}-{uid}.{region}.myqcloud.com/{path}".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region,
                path=to_unicode(path)
            )
        else:
            url = u"http://{bucket}-{uid}.{region}.myqcloud.com".format(
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
        self._retry = 1
        self._file_num = 0
        self._folder_num = 0
        self._path_md5 = ""
        self._have_uploaded = []
        self._etag = 'ETag'
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

    def list_part(self, cos_path):
        logger.info("getting uploaded parts")
        NextMarker = ""
        IsTruncated = "true"
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
                logger.warn(response_info("get res", rt))
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
        if len(filelist) == 0:
            self._file_num += 1
            logger.debug(cos_path+'tmp/')
            self.upload_file(local_path="", cos_path=cos_path+"tmp/")
        for filename in filelist:
            filepath = os.path.join(local_path, filename)
            if os.path.isdir(filepath):
                self.upload_folder(filepath, cos_path+filename)
            else:
                if self.upload_file(local_path=filepath, cos_path=cos_path+filename) is False:
                    logger.info("upload {file} fail".format(file=to_printable_str(filepath)))
                else:
                    self._file_num += 1
                    logger.debug("upload {file} success".format(file=to_printable_str(filepath)))

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
                        logger.warn(response_info("put res", rt))
                        continue
                    if j+1 == self._retry:
                        return False
                except Exception:
                    logger.warn("upload file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._have_finished = 0
            self._have_uploaded = []
            logger.info("checking upload breakpoint...")
            self._path_md5 = get_md5_filename(local_path, cos_path)
            logger.debug("init with : " + url)
            if os.path.isfile(self._path_md5):
                with open(self._path_md5, 'rb') as f:
                    self._upload_id = f.read()
                if self.list_part(cos_path) is True:
                    logger.info("continue uploading from last breakpoint")
                    return True
                else:
                    logger.info("read breakpoint fail, start uploading again")
            else:
                logger.info("can not find upload breakpoint")
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
                logger.warn(response_info("post res", rt))
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
                        view_bar(self._have_finished, parts_size)
                        break
                    else:
                        logger.warn(response_info("put res", rt))
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
            while file_size / chunk_size > 10000:
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
            if chunk_size >= file_size:
                pool.add_task(multiupload_parts_data, local_path, offset, file_size, 1, 0)
            else:
                for i in range(parts_num):
                    if(str(i+1) in self._have_uploaded):
                        offset += chunk_size
                        continue
                    if i+1 == parts_num:
                        pool.add_task(multiupload_parts_data, local_path, offset, file_size-offset, parts_num, i+1)
                    else:
                        pool.add_task(multiupload_parts_data, local_path, offset, chunk_size, parts_num, i+1)
                        offset += chunk_size
            pool.wait_completion()
            result = pool.get_result()
            print ""
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
                    logger.warn(response_info("post res", rt))
                    return False
            except Exception:
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

    def download_file(self, local_path, cos_path):

        url = self._conf.uri(path=cos_path)
        logger.info("download with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))

            if 'Content-Length' in rt.headers:
                content_len = int(rt.headers['Content-Length'])
            else:
                raise IOError("download failed without Content-Length header")
            if rt.status_code == 200:
                file_len = 0
                with open(local_path, 'wb') as f:
                    for chunk in rt.iter_content(chunk_size=1024):
                        if chunk:
                            file_len += len(chunk)
                            f.write(chunk)
                    f.flush()
                if file_len != content_len:
                    raise IOError("download failed with incomplete file")
                return True
            else:
                logger.warn(response_info("get res", rt))
                return False
        except Exception:
            logger.warn("Error!")
            return False
        return False

    def delete_folder(self, cos_path):

        def multidelete_parts_data(_cos_path):
            for i in range(self._retry):
                logger.debug("delete object with : " + _cos_path)
                url_file = self._conf.uri(path=_cos_path)
                rt = self._session.delete(url=url_file, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
                if rt.status_code == 204:
                    self._have_finished += 1
                    view_bar(self._have_finished, self._file_num)
                    break
        cos_path = to_unicode(cos_path)
        if len(cos_path) > 0:
            if cos_path[-1] != '/':
                cos_path += '/'
        self._have_finished = 0
        self._file_num = 0
        NextMarker = ""
        IsTruncated = "true"
        pagecount = 0
        file_list = []
        logger.info("getting folder...")
        while IsTruncated == "true":
            pagecount += 1
            url = self._conf.uri(path='?max-keys=1000&marker={nextmarker}&prefix={prefix}'.format(nextmarker=NextMarker, prefix=cos_path))
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
                contentset = root.getElementsByTagName("Key")
                for content in contentset:
                    self._file_num += 1
                    file_name = content.childNodes[0].data
                    file_list.append(file_name)
            else:
                logger.warn(response_info("get res", rt))
                logger.debug("get folder error")
                return False
        logger.info("filecount: %d" % (self._file_num))
        # make sure
        if query_yes_no("you are deleting the cos_path '{cos_path}', please make sure".format(cos_path=cos_path)) is False:
            return False
        logger.info("deleting folder...")
        _max_thread = min(self._conf._max_thread, self._file_num)
        pool = SimpleThreadPool(_max_thread)
        for cos_path in file_list:
            pool.add_task(multidelete_parts_data, cos_path)
        pool.wait_completion()
        print ""
        logger.info("deleted: %d" % (self._have_finished))
        if self._file_num == self._have_finished:
            logger.debug("get folder success")
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
            if rt.status_code == 204:
                return True
            else:
                logger.warn(response_info("delete res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("get res", rt))
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
                logger.warn(response_info("put res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("get res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("put res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("delete res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("get res", rt))
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
                logger.warn(response_info("get res", rt))
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
                logger.warn(response_info("put res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
                logger.warn(response_info("get res", rt))
                return False
        except Exception:
            logger.warn("Error!")
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
