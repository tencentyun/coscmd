# -*- coding: utf-8 -*-
from urlparse import urlparse
import hashlib
import hmac
import time
import urllib
from urllib import quote
import requests
from requests.auth import AuthBase
import logging
logger = logging.getLogger(__name__)


class CosS3Auth(AuthBase):

    def __init__(self, access_id, secret_key, expire=10000):
        self._access_id = access_id
        self._secret_key = secret_key
        self._expire = expire

    def __call__(self, r):
        method = r.method.lower()
        uri = r.url

        rt = urlparse(uri)
        logger.debug("url parse: " + str(rt))
        if rt.query != "" and ("&" in rt.query or '=' in rt.query):
            uri_params = dict(map(lambda s: s.lower().split('='), rt.query.split('&')))
        elif rt.query != "":
            uri_params = {rt.query: ""}
        else:
            uri_params = {}
        del r.headers["accept"]
        del r.headers["accept-encoding"]
        del r.headers["connection"]
        del r.headers["user-agent"]

        r.headers['Host'] = rt.netloc
        headers = dict([(k.lower(), quote(v).lower()) for k, v in r.headers.items()])

        format_str = "{method}\n{host}\n{params}\n{headers}\n".format(
            method=method.lower(),
            host=rt.path,
            params=urllib.urlencode(uri_params),
            headers='&'.join(map(lambda (x,y): "%s=%s"%(x,y), sorted(headers.items())))
        )
        logger.debug("format str: " + format_str)

        start_sign_time = int(time.time())
        sign_time = "{bg_time};{ed_time}".format(bg_time=start_sign_time-60, ed_time=start_sign_time + self._expire)
        # sign_time = "1480932292;1481012292"
        sha1 = hashlib.sha1()
        sha1.update(format_str)

        str_to_sign = "sha1\n{time}\n{sha1}\n".format(time=sign_time, sha1=sha1.hexdigest())
        logger.debug('str_to_sign: ' + str(str_to_sign))
        sign_key = hmac.new(self._secret_key, sign_time, hashlib.sha1).hexdigest()
        sign = hmac.new(sign_key, str_to_sign, hashlib.sha1).hexdigest()
        logger.debug('sign_key: ' + str(sign_key))
        logger.debug('sign: ' + str(sign))
        sign_tpl = "q-sign-algorithm=sha1&q-ak={ak}&q-sign-time={sign_time}&q-key-time={key_time}&q-header-list={headers}&q-url-param-list={params}&q-signature={sign}"
        r.headers['Authorization'] = sign_tpl.format(
            ak=self._access_id,
            sign_time=sign_time,
            key_time=sign_time,
            params=';'.join(sorted(map(lambda k: k.lower(), uri_params.keys()))),
            headers=';'.join(sorted(headers.keys())),
            sign=sign
        )
        logger.debug("request headers: " + str(r.headers))
        return r

if __name__ == "__main__":
    import logging
    import sys
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    resp = requests.head("http://sdktestgz-1252448703.cn-south.myqcloud.com/config.json", auth=CosS3Auth("AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00", "ciivKvnnrMvSvQpMAWuIz12pThGGlWRW"))
    #resp = requests.get("http://testbucket-125000000.cn-north.myqcloud.com/testfile", headers={"Range": "bytes=0-3"}, auth=CosS3Auth("QmFzZTY0IGlzIGEgZ2VuZXJp", "AKIDZfbOA78asKUYBcXFrJD0a1ICvR98JM"))
    print resp.status_code, resp.text
    f = open("Client.py", "r")
    print "UPLOAD"
    t = requests.put("http://sdktestgz-1252448703.cn-south.myqcloud.com/client2.py", auth=CosS3Auth("AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00", "ciivKvnnrMvSvQpMAWuIz12pThGGlWRW"), data="helasdfasf")
    print "UPLOAD"
    print t.status_code, t.text

