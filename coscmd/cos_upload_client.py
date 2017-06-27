# -*- coding=utf-8
from cos_upload_auth import CosS3Auth
from cos_upload_threadpool import SimpleThreadPool
from urllib import quote
import time
import requests
from os import path
from hashlib import md5
from contextlib import closing
from xml.dom import minidom
import logging
import time
import sys
import os
logger = logging.getLogger(__name__)


class CosConfig(object):
    def __init__(self, appid, region, bucket, access_id, access_key, part_size=1, max_thread=5, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._access_id = access_id
        self._access_key = access_key
        self._part_size = min(part_size,10)
        self._max_thread = min(max_thread,10)
        
        logger.warn("config parameter:\nappid: {appid}, region: {region}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
                 appid = appid,
                 region = region,
                 bucket = bucket,
                 part_size = part_size,
                 max_thread = max_thread))
    def uri(self, path=None):
        if path:
            return "http://{bucket}-{uid}.{region}.myqcloud.com/{path}".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region,
                path=quote(path)
            )
        else:
            return "http://{bucket}-{uid}.{region}.myqcloud.com".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region
            )


class MultiPartUpload(object):

    def __init__(self, filename, object_name, conf, session=None):
        self._filename = filename
        self._object_name = object_name
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0;
        self._err_tips = ''
        if session is None:
          self._session = requests.session()
        else:
          self._session = session
          
    def check_local_file_valid(self, local_path):
        if not os.path.exists(local_path):
            self._err_tips = 'local_file %s not exist!' % local_path
            return False
        if not os.path.isfile(local_path):
            self._err_tips = 'local_file %s is not regular file!' % local_path
            return False
        if not os.access(local_path, os.R_OK):
            self._err_tips = 'local_file %s is not readable!' % local_path
            return False
        return True
    
    def init_mp(self):
        
        if self.check_local_file_valid(self._filename) == False:
            logger.warn(self._err_tips)
            return False
        
        url = self._conf.uri(path=self._object_name)
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
            logger.warn("init upload part failed.")
            return False
        return True
    
    def upload_parts(self):
        #读文件的偏移量
        offset = 0
        file_size = path.getsize(self._filename)
        logger.info("file size: " + str(file_size))
        chunk_size = 1024 * 1024 * self._conf._part_size
        #防止分块太多
        while file_size / chunk_size > 10000:
            chunk_size = chunk_size * 10
        #块的数量
        parts_num = file_size / chunk_size
        #最后一个块的大小
        last_size = file_size - parts_num * chunk_size 
        if last_size != 0:
            parts_num += 1
        self._md5 = range(parts_num);
        #若分块太少，限制线程
        if parts_num < self._conf._max_thread:
            self._conf._max_thread = parts_num
        pool = SimpleThreadPool(self._conf._max_thread)
        
        logger.info("chunk_size: " + str(chunk_size))
        logger.info('upload file concurrently')
        logger.warn("upload {file} with {per}%".format(file=self._filename, per="{0:5.2f}".format(0.00)))
        #单文件小于分块大小
        if chunk_size >= file_size:
            pool.add_task(self.upload_parts_data, self._filename, offset, file_size, 1, 0)
        #分块上传
        else:
            for i in range(parts_num):
                #最后一个不满的
                if i+1 == parts_num:
                    pool.add_task(self.upload_parts_data, self._filename, offset, file_size-offset-1, parts_num, i)
                else:
                    pool.add_task(self.upload_parts_data, self._filename, offset, chunk_size, parts_num, i)
                    offset+=chunk_size
            
        pool.wait_completion()
        result = pool.get_result()
        if result['success_all']:
            return True
        else:
            return False    
        
    def upload_parts_data(self, filename, offset, len, parts_size, idx, retry=5):
        with open(filename, 'rb') as file:
            file.seek(offset,0)
            data = file.read(len);
        url = self._conf.uri(path=self._object_name)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx+1, uploadid=self._upload_id)
        logger.info("upload url: " + str(url))
        md5_etag = md5()
        md5_etag.update(data)
        self._md5[idx]=md5_etag.hexdigest()
        for j in range(retry):
            try:
                rt = self._session.put(url=url,
                                       auth=CosS3Auth(self._conf._access_id, self._conf._access_key),
                                       data=data)
                logger.info("multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                    part = idx+1,
                    round = j+1,
                    code=rt.status_code,
                    headers=rt.headers,
                    text=rt.text))            
                if rt.status_code == 200:
                    if 'ETag' in rt.headers:
                        if rt.headers['ETag'] != '"%s"' % md5_etag.hexdigest():
                            logger.warn("upload file {file} response with error etag : {etag1}, {etag}".format(file=self._filename, etag=rt.headers['ETag'], etag1='%s' % md5_etag.hexdigest()))
                            continue
                        else:
                            self._have_finished+=1
                            logger.warn("upload {file} with {per}%".format(file=self._filename, per="{0:5.2f}".format(self._have_finished*100/float(parts_size))))
                            break
                    else:
                        logger.warn("upload file {file} response with no etag ".format(file=self._filename))
                        continue
                else:
                    time.sleep(2**j)
                    continue;
                if j+1 == retry:
                    logger.exception("upload part failed: part{part}, round{round}, code: {code}".format(
                    part = idx+1,
                    round = j+1,
                    code=rt.status_code))
                    return False
                
            except Exception:
                logger.exception("upload part failed")
        return True
        
    def complete_mp(self):
        doc = minidom.Document()
        root = doc.createElement("CompleteMultipartUpload")
        #root = etree.Element("CompleteMultipartUpload")
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
            url = self._conf.uri(path=self._object_name)+"?uploadId={uploadid}".format(uploadid=self._upload_id)
            logger.debug('complete url: ' + str(url))
            logger.debug("complete data: " + str(data))
        try:
            with closing(self._session.post(url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key), data=data, stream=True)) as rt:
                logger.debug("complete status code: {code}".format(code=rt.status_code))
                logger.debug("complete headers: {headers}".format(headers=rt.headers))
            return rt.status_code == 200
        except Exception:
            logger.warn("complete upload part failed.")
            return False
        return True

class CosS3Client(object):

    def __init__(self, conf):
        self._conf = conf
        self._session = requests.session()

    def put_object_from_filename(self, filename, object_name):
        url = self._conf.uri(object_name)

        from os.path import exists
        if exists(filename):
            with open(filename, 'r') as f:
                rt = self._session.put(url, data=f.read(), auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
                return rt.status_code == 200
        else:
            raise IOError("{f} doesn't exist".format(f=filename))

    def multipart_upload_from_filename(self, filename, object_name):
        return MultiPartUpload(filename=filename, object_name=object_name, conf=self._conf, session=self._session)


if __name__ == "__main__":
    
    logging.basicConfig(level=logging.WARN, stream=sys.stdout, format="%(asctime)s - %(message)s")
    conf = CosConfig(appid="1252448703",
                     bucket="uploadtest",
                     region="cn-north",
                     access_id="AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00",
                     access_key="*",
                     part_size=1,
                     max_thread=5,                                          )

    client = CosS3Client(conf)

    mp = client.multipart_upload_from_filename('1.txt', "1.txt")
    rt_init = mp.init_mp()
    rt_part = mp.upload_parts()
    rt_mp = mp.complete_mp()
    if rt_init and rt_part and rt_mp  == True:
        print ("success!")
    else:
        print ("fail!")


