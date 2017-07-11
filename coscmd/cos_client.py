# -*- coding=utf-8
from cos_auth import CosS3Auth
from cos_threadpool import SimpleThreadPool
from urllib import quote
import time
import requests
from os import path
from hashlib import md5
from contextlib import closing
from xml.dom import minidom
import logging
import random
import time
import sys
import os
logger = logging.getLogger(__name__)


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


class CosConfig(object):

    def __init__(self, appid, region, bucket, access_id, access_key, part_size=1, max_thread=5, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._access_id = access_id
        self._access_key = access_key
        self._part_size = min(10, part_size)
        self._max_thread = min(10, max_thread)
        logger.info("config parameter-> appid: {appid}, region: {region}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
                 appid=appid,
                 region=region,
                 bucket=bucket,
                 part_size=part_size,
                 max_thread=max_thread))

    def uri(self, path=None):
        if path:
            url = "http://{bucket}-{uid}.{region}.myqcloud.com/{path}".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region,
                path=path
            )
        else:
            url = "http://{bucket}-{uid}.{region}.myqcloud.com".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region
            )
        return url


class ObjectInterface(object):

    def __init__(self, conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0
        self._err_tips = ''
        self._retry = 2
        self._file_num = 0
        self._folder_num = 0
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

    def upload_folder(self, local_path, cos_path):
        local_path = local_path.decode('utf-8')
        cos_path = cos_path.decode('utf-8')
        filelist = os.listdir(local_path)
        self._folder_num += 1
        if len(filelist) == 0:
            logger.debug(cos_path+'/'+'tmp/')
            self.upload_file(local_path="", cos_path=cos_path+'/'+"tmp/")
        for filename in filelist:
            filepath = os.path.join(local_path, filename)
            if os.path.isdir(filepath):
                self.upload_folder(filepath, cos_path+'/'+filename)
            else:
                logger.debug(str(filepath)+" " + str(cos_path)+'/'+str(filename))
                if self.upload_file(local_path=filepath, cos_path=cos_path+'/'+filename) is False:
                    logger.info("upload {file} fail".format(file=filepath))
                else:
                    self._file_num += 1
                    logger.debug("upload {file} success".format(file=filepath))

    def upload_file(self, local_path, cos_path):

        def single_upload():
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as file:
                    data = file.read()
            url = self._conf.uri(path=cos_path)
            for j in range(self._retry):
                try:
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data)
                    if rt.status_code == 200:
                        if local_path != '':
                            logger.warn("upload {file} with {per}%".format(file=local_path, per="{0:5.2f}".format(100)))
                        return True
                    else:
                        time.sleep(2**j)
                        continue
                    if j+1 == self._retry:
                        return False
                except Exception:
                    logger.exception("upload file failed")
            return False

        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._have_finished = 0
            logger.debug("init with : " + url)
            try:
                rt = self._session.post(url=url+"?uploads", auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
                logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                     code=rt.status_code,
                     headers=rt.headers,
                     text=rt.text))

                root = minidom.parseString(rt.content).documentElement
                self._upload_id = root.getElementsByTagName("UploadId")[0].childNodes[0].data
                return rt.status_code == 200
            except Exception:
                return False
            return Tr

        def multiupload_parts():

            def multiupload_parts_data(local_path, offset, len, parts_size, idx):
                with open(local_path, 'rb') as file:
                    file.seek(offset, 0)
                    data = file.read(len)
                url = self._conf.uri(path=cos_path)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx+1, uploadid=self._upload_id)
                logger.debug("upload url: " + str(url))
                md5_ETag = md5()
                md5_ETag.update(data)
                self._md5[idx] = md5_ETag.hexdigest()
                for j in range(self._retry):
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._access_id, self._conf._access_key),
                                           data=data)
                    logger.debug("multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                        part=idx+1,
                        round=j+1,
                        code=rt.status_code,
                        headers=rt.headers,
                        text=rt.text))
                    if rt.status_code == 200:
                        self._have_finished += 1
                        view_bar(self._have_finished, parts_size)
                        break
                    else:
                        time.sleep(2**j)
                        continue
                    if j+1 == retry:
                        logger.exception("upload part failed: part{part}, round{round}, code: {code}".format(part=idx+1, round=j+1, code=rt.status_code))
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
            if last_size != 0:
                parts_num += 1
            self._md5 = range(parts_num)
            if parts_num < self._conf._max_thread:
                self._conf._max_thread = parts_num
            pool = SimpleThreadPool(self._conf._max_thread)
            logger.debug("chunk_size: " + str(chunk_size))
            logger.debug('upload file concurrently')
            logger.info("uploading {file}".format(file=local_path))
            if chunk_size >= file_size:
                pool.add_task(multiupload_parts_data, local_path, offset, file_size, 1, 0)
            else:
                for i in range(parts_num):
                    if i+1 == parts_num:
                        pool.add_task(multiupload_parts_data, local_path, offset, file_size-offset-1, parts_num, i)
                    else:
                        pool.add_task(multiupload_parts_data, local_path, offset, chunk_size, parts_num, i)
                        offset += chunk_size
            pool.wait_completion()
            result = pool.get_result()
            print ""
            if result['success_all']:
                return True
            else:
                return False

        def complete_multiupload():
            doc = minidom.Document()
            root = doc.createElement("CompleteMultipartUpload")
            for i, v in enumerate(self._md5):
                t = doc.createElement("Part")
                t1 = doc.createElement("PartNumber")
                t1.appendChild(doc.createTextNode(str(i+1)))

                t2 = doc.createElement("ETag")
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
                return rt.status_code == 200
            except Exception:
                return False
            return True

        logger.debug("file_path-> local_path: {local_path}, cos_path: {cos_path}".format(
               local_path=local_path.encode('utf-8').encode('gbk'),
               cos_path=cos_path))
        if local_path == "":
            file_size = 0
        else:
            file_size = os.path.getsize(local_path.decode('utf-8'))
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

        if cos_path.startswith("cos://"):
            cos_path = cos_path.split("cos://")[1]
            bucket_name = cos_path.split('-')[0]
            cos_path = cos_path[len(bucket_name)+1:]
            app_id = cos_path.split('.')[0]
            cos_path = cos_path[len(app_id)+1:]
            region = cos_path.split(".")[0]
            cos_path = cos_path[len(region+".myqcloud.com/"):]

            try:
                tmp = self._conf
                self._conf._bucket = bucket_name
                self._conf._appid = app_id
                self._conf._region = region
                url = self._conf.uri(path=cos_path)
                logger.info("download with : " + url)
                rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
                logger.debug("init resp, status code: {code}, headers: {headers}".format(
                     code=rt.status_code,
                     headers=rt.headers))
                self._conf = tmp
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
                    logger.warn(rt.content)
                    return False
            except Exception:
                self._conf = tmp
                logger.exception("Error!")
                return False
            return False
        else:
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
                    logger.warn(rt.content)
                    return False
            except Exception:
                logger.exception("Error!")
                return False
            return False

    def delete_file(self, cos_path):
        url = self._conf.uri(path=cos_path)
        logger.info("delete with : " + url)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            return rt.status_code == 204
        except Exception:
            logger.exception("Error!")
            return False
        return False


class BucketInterface(object):

    def __init__(self,  conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0
        self._err_tips = ''
        if session is None:
            self._session = requests.session()
        else:
            self._session = session

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
            return rt.status_code == 200
        except Exception:
            logger.exception("Error!")
            return False
        return True

    def delete_bucket(self):
        url = self._conf.uri(path='')
        self._have_finished = 0
        logger.debug("delete bucket with : " + url)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("put resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))
            return rt.status_code == 200
        except Exception:
            logger.exception("Error!")
            return False
        return True

    def get_bucket(self):
        NextMarker = ""
        IsTruncated = "true"
        pagecount = 0
        filecount = 0
        sizecount = 0
        with open('tmp.xml', 'wb') as f:
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
                         text=rt.text))
                    contentset = root.getElementsByTagName("Contents")
                    for content in contentset:
                        filecount += 1
                        sizecount += int(content.getElementsByTagName("Size")[0].childNodes[0].data)
                        f.write(content.toxml())
                else:
                    logger.debug("get bucket error")
                    return False

        logger.info("filecount: %d" % filecount)
        logger.info("sizecount: %d" % sizecount)
        logger.debug("get bucket success")
        return True


class CosS3Client(object):

    def __init__(self, conf):
        self._conf = conf
        self._session = requests.session()

    def obj_int(self, local_path='', cos_path=''):
        return ObjectInterface(conf=self._conf, session=self._session)

    def buc_int(self):
        return BucketInterface(conf=self._conf, session=self._session)


if __name__ == "__main__":
    pass
