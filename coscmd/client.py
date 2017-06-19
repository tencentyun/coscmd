# -*- coding=utf-8

from __future__ import absolute_import
# from coscmd.signaturer import Signature
from coscmd.auth import CosS3Auth
from urllib import quote
import time
import requests
from lxml import etree
from os import path
from hashlib import sha1
from contextlib import closing
import logging
import time
logger = logging.getLogger(__name__)


class CosConfig(object):
    def __init__(self, appid, region, bucket, access_id, access_key, part_size, *args, **kwargs):
        self._appid = appid
        self._region = region
        self._bucket = bucket
        self._access_id = access_id
        self._access_key = access_key
        self._part_size = part_size

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

    def __init__(self, filename, object_name, conf, session=None, verify_etag=False):
        self._filename = filename
        self._object_name = object_name
        self._conf = conf
        self._upload_id = None
        self._sha1 = []
        self._verify_etag = verify_etag

        if session is None:
          self._session = requests.session()
        else:
          self._session = session

    def init_mp(self):
        url = self._conf.uri(path=self._object_name)
        logger.debug("init with : " + url)
        try:
            rt = self._session.post(url=url+"?uploads", auth=CosS3Auth(self._conf._access_id, self._conf._access_key))
            logger.debug("init resp, status code: {code}, headers: {headers}, text: {text}".format(
                 code=rt.status_code,
                 headers=rt.headers,
                 text=rt.text))
            root = etree.XML(rt.content)
            self._upload_id = root.getchildren()[2].text
            return rt.status_code == 200
        except Exception:
            logger.warn("init upload part failed.")
            return False

    def upload_parts(self):
      self._sha1 = [] 
      file_size = path.getsize(self._filename)
      logger.info("file size: " + str(file_size))
      # 50 parts, max chunk size 5 MB
      # chunk_size = 10 * 1024 * 1024 # 10 MB
      chunk_size = 1024 * 1024 * self._conf._part_size
      while file_size / chunk_size > 10000:
	chunk_size = chunk_size * 10 

      parts_size = (file_size + chunk_size - 1) / chunk_size  
      logger.info("chunk_size: " + str(chunk_size))
      logger.info("parts_size: " + str((file_size + chunk_size - 1)/chunk_size))
      
      # use binary mode to fix windows bug
      with open(self._filename, 'rb') as f:     
	# /ObjectName?partNumber=PartNumber&uploadId=UploadId
	for i in range(parts_size):
	  data = f.read(chunk_size)
	  sha1_etag = sha1()
	  sha1_etag.update(data)
          # self._sha1.append(sha1_etag.hexdigest())
          url = self._conf.uri(path=self._object_name)+"?partNumber={partnum}&uploadId={uploadid}".format(partnum=i+1, uploadid=self._upload_id)
	  logger.debug("upload url: " + str(url))
	  for j in range(5):
	    try:
	      rt = self._session.put(url=url,
	      		   auth=CosS3Auth(self._conf._access_id, self._conf._access_key),
      			   data=data)
	      logger.info("multi part resul, code: {code}, headers: {headers}, text: {text}".format(
		      code=rt.status_code,
		      headers=rt.headers,
		      text=rt.text))
	      if rt.status_code == 200:
                if 'Etag' in rt.headers:
                    _etag = rt.headers['Etag']
                    if self._verify_etag:
                        assert 0 == "etag verify is not supported now!"
                    else:
                        self._sha1.append(_etag)
	                logger.warn("upload {file} with {per}%".format(file=self._filename, per="{0:5.2f}".format(i*100/float(parts_size))))
                        break
                else:
                   logger.warn("upload file {file} response with no etag ".format(file=self._filename))
                   continue
              elif rt.status_code == 503:
                time.sleep(2**j)
	    except Exception:
	      logger.exception("upload part failed")
          else:
	    return False

      logger.warn("upload {file} with 100.00%".format(file=self._filename))
      return True

    def complete_mp(self):
	root = etree.Element("CompleteMultipartUpload")
	for i, v in enumerate(self._sha1):
	  t = etree.Element("Part")
	  t1 = etree.Element("PartNumber")
	  t1.text = str(i+1)

	  t2 = etree.Element("ETag")
	  t2.text = v

	  t.append(t1)
	  t.append(t2)
	  root.append(t)
	data = etree.tostring(root)
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

    conf = CosConfig(appid="1252448703",
                     bucket="sdktestgz",
                     region="cn-south",
                     access_id="AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00",
                     access_key="ciivKvnnrMvSvQpMAWuIz12pThGGlWRW")

    client = CosS3Client(conf)

    # rt = client.put_object_from_filename("auth.py", "auth1.py")
    # print rt

    mp = client.multipart_upload_from_filename("auth.py", "auth2asdf.py")
    print mp.init_mp()
    mp.upload_parts()
    mp.complete_mp()

