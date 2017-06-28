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

class CosConfig(object):
    
    def __init__(self, appid, region, bucket, access_id, access_key, part_size=1, max_thread=5, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._access_id = access_id
        self._access_key = access_key
        self._part_size = min(10,part_size)
        self._max_thread = min(10,max_thread)
        logger.debug("config parameter-> appid: {appid}, region: {region}, bucket: {bucket}, part_size: {part_size}, max_thread: {max_thread}".format(
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
                path=path
            )
        else:
            return "http://{bucket}-{uid}.{region}.myqcloud.com".format(
                bucket=self._bucket,
                uid=self._appid,
                region=self._region
            )

class ObjectInterface(object):

    def __init__(self, conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0;
        self._err_tips = ''
        self._retry = 2
        self._file_num = 0
        self._folder_num = 0
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

    def check_local_folder_valid(self, local_path):
        if not os.path.exists(local_path):
            self._err_tips = 'local_folder %s not exist!' % local_path
            return False
        if not os.path.isdir(local_path):
            self._err_tips = 'local_folder %s is not regular file!' % local_path
            return False
        if not os.access(local_path, os.R_OK):
            self._err_tips = 'local_folder %s is not readable!' % local_path
            return False
        return True
    
    #文件夹上传
    def upload_folder(self, local_path, cos_path):
        local_path = local_path.decode('utf-8')
        cos_path = cos_path.decode('utf-8')
        filelist = os.listdir(local_path)
        self._folder_num += 1
        if len(filelist) == 0:
            logger.debug(cos_path+'/'+'tmp/')
            self.upload_file(local_path="", cos_path=cos_path+'/'+"tmp/")
        for filename in filelist:
            filepath = os.path.join(local_path,filename)  
            if os.path.isdir(filepath):  
                self.upload_folder(filepath,cos_path+'/'+filename) 
            else: 
                logger.debug(str(filepath)+" " + str(cos_path)+'/'+str(filename))
                if self.upload_file(local_path=filepath, cos_path=cos_path+'/'+filename) == False:
                    logger.info("upload {file} fail".format(file=filepath))
                else:
                    self._file_num += 1
                    logger.debug("upload {file} success".format(file=filepath))
    
    #文件上传
    def upload_file(self, local_path, cos_path):
        
        #单文件
        def single_upload():
            #空文件夹
            if len(local_path) == 0:
                data = ""
            else:
                with open(local_path, 'rb') as file:
                    data = file.read();
            url = self._conf.uri(path=cos_path)
            #发送请求
            for j in range(self._retry):
                try:
                    rt = self._session.put(url=url,
                                           auth=CosS3Auth(self._conf._access_id, self._conf._access_key),data=data)
                    if rt.status_code == 200:
                        if local_path != '':
                            logger.warn("upload {file} with {per}%".format(file=local_path, per="{0:5.2f}".format(100)))
                        return True
                    else:
                        time.sleep(2**j)
                        continue;
                    if j+1 == self._retry:
                        return False
                except Exception:
                    logger.exception("upload file failed")
            return False
        
        #初始化分块
        def init_multiupload():
            url = self._conf.uri(path=cos_path)
            self._have_finished = 0;
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
            return True
        
        #分块上传
        def multiupload_parts():
            
             #线程函数
            def multiupload_parts_data(local_path, offset, len, parts_size, idx):
                with open(local_path, 'rb') as file:
                    file.seek(offset,0)
                    data = file.read(len);
                url = self._conf.uri(path=cos_path)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=idx+1, uploadid=self._upload_id)
                logger.debug("upload url: " + str(url))
                md5_ETag = md5()
                md5_ETag.update(data)
                self._md5[idx]=md5_ETag.hexdigest()
                for j in range(self._retry):
                    try:
                        rt = self._session.put(url=url,
                                               auth=CosS3Auth(self._conf._access_id, self._conf._access_key),
                                               data=data)
                        logger.debug("multi part result: part{part}, round{round}, code: {code}, headers: {headers}, text: {text}".format(
                            part = idx+1,
                            round = j+1,
                            code=rt.status_code,
                            headers=rt.headers,
                            text=rt.text))            
                        if rt.status_code == 200:
                            if 'ETag' in rt.headers:
                                if rt.headers['ETag'] != '"%s"' % md5_ETag.hexdigest():
                                    logger.warn("upload file {file} response with error ETag : {ETag1}, {ETag}".format(file=self._filename, ETag=rt.headers['ETag'], ETag1='%s' % md5_ETag.hexdigest()))
                                    continue
                                else:
                                    self._have_finished+=1
                                    logger.warn("upload {file} with {per}%".format(file=local_path, per="{0:5.2f}".format(self._have_finished*100/float(parts_size))))
                                    break
                            else:
                                logger.warn("upload file {file} response with no ETag ".format(file=self._filename))
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
            
            #读文件的偏移量
            offset = 0
            file_size = path.getsize(local_path)
            logger.debug("file size: " + str(file_size))
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
            logger.debug("chunk_size: " + str(chunk_size))
            logger.debug('upload file concurrently')
            logger.info("upload {file} with {per}%".format(file=local_path, per="{0:5.2f}".format(0)))               
            #单文件小于分块大小
            if chunk_size >= file_size:
                pool.add_task(multiupload_parts_data, local_path, offset, file_size, 1, 0)
            #分块上传
            else:
                for i in range(parts_num):
                    #最后一个不满的
                    if i+1 == parts_num:
                        pool.add_task(multiupload_parts_data, local_path, offset, file_size-offset-1, parts_num, i)
                    else:
                        pool.add_task(multiupload_parts_data, local_path, offset, chunk_size, parts_num, i)
                        offset+=chunk_size
            #等待结束
            pool.wait_completion()
            result = pool.get_result()
            if result['success_all']:
                return True
            else:
                return False    
        #总结分块
        def complete_multiupload():
            #解析返回xml
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
        
        #上传分类
        logger.debug("file_path-> local_path: {local_path}, cos_path: {cos_path}".format(
               local_path = local_path.encode('utf-8').encode('gbk'),
               cos_path = cos_path))
        #获得非空文件大小
        if local_path == "":
            file_size = 0
        else:
            file_size = os.path.getsize(local_path.decode('utf-8'))
        #按大小区分上传方式
        #单文件上传
        if file_size < 5*1024*1024 :
            for i in range(self._retry):
                if single_upload() == True:
                    return True
            return False
                #rt = single_upload()
        #分块多线程
        else:
            for i in range(self._retry):
                
                rt = init_multiupload()
                if rt:
                    break
                wait_time = random.randint(0, 1)
                logger.debug("begin to init upload part after {second} second".format(second=wait_time))
                time.sleep(wait_time)
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
                wait_time = random.randint(0, 1)
                time.sleep(wait_time)
                logger.debug("begin to complete upload part after {second} second".format(second=wait_time))
            logger.warn("complete multipart upload failed")
            return False
    
    #下载
    def download_file(self):
        url = self._conf.uri(path=cos_path)
        logger.debug("download with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            
            if 'Content-Length' in rt.headers:
                content_len = int(rt.headers['Content-Length'])
            else:
                raise IOError("download failed without Content-Length header")
            file_len = 0
            with open(local_path, 'wb') as f:
                for chunk in rt.iter_content(chunk_size=1024):
                    if chunk:
                        file_len += len(chunk)
                        f.write(chunk)
                f.flush()
            if file_len != content_len:
                raise IOError("download failed with incomplete file")
            return rt.status_code == 200
        except Exception:
            logger.exception("Error!")
            return False
        return True
    
    def delete_file(self):
        url = self._conf.uri(path=cos_path)
        logger.debug("delete with : " + url)
        try:
            rt = self._session.delete(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}".format(
                 code=rt.status_code,
                 headers=rt.headers))
            return rt.status_code == 204
        except Exception:
            logger.exception("Error!")
            return False
        return True
    
class BucketInterface(object):

    def __init__(self,  conf, session=None):
        self._conf = conf
        self._upload_id = None
        self._md5 = []
        self._have_finished = 0;
        self._err_tips = ''
        if session is None:
            self._session = requests.session()
        else:
            self._session = session
          
    
    def get_bucket(self):
        
        url = self._conf.uri(path='')
        self._have_finished = 0;
        logger.debug("get with : " + url)
        try:
            rt = self._session.get(url=url, auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))
            print rt.content
            root = minidom.parseString(rt.content).documentElement
#             for obj in root.getElementsByTagName("Contents"):
#                 for tmp in obj.childNode:
#                     print tmp
            return rt.status_code == 200
        except Exception:
            logger.exception("Error!")
            return False
        return True
   
class CosS3Client(object):

    def __init__(self, conf):
        self._conf = conf
        self._session = requests.session()
    
    #object接口
    def obj_int(self, local_path='', cos_path=''):
        return ObjectInterface(conf=self._conf, session=self._session)
    def buc_int(self):
        return BucketInterface(conf=self._conf, session=self._session)
    

if __name__ == "__main__":
    
    pass