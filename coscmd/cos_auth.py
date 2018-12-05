# -*- coding: utf-8 -*-
import hmac
import time
import urllib
import hashlib
import logging
import requests
import sys
from six.moves.urllib.parse import quote, urlparse, unquote, urlencode
from requests.auth import AuthBase

if sys.version > '3':
    from coscmd.cos_global import Version
    from coscmd.cos_comm import to_bytes
else:
    from cos_global import Version
    from cos_comm import to_bytes

logger = logging.getLogger("coscmd")


class CosS3Auth(AuthBase):

    def __init__(self, conf, expire=10000):
        self._access_id = conf._secret_id
        self._secret_key = conf._secret_key
        self._anonymous = conf._anonymous
        self._expire = expire

    def __call__(self, r):
        method = r.method.lower()
        uri = r.url
        uri = uri.split('?')[0]
        tmp_r = {}
        rt = urlparse(uri)
        logger.debug("url parse: " + str(rt))
        if rt.query != "" and ("&" in rt.query or '=' in rt.query):
            uri_params = dict(map(lambda s: s.lower().split('='), rt.query.split('&')))
        elif rt.query != "":
            uri_params = {rt.query: ""}
        else:
            uri_params = {}
        tmp_r = {}
        tmp_r = r.headers
        r.headers = {}
        r.headers['Host'] = rt.netloc
        headers = dict([(k.lower(), quote(v).lower()) for k, v in r.headers.items()])
        format_str = "{method}\n{host}\n{params}\n{headers}\n".format(
            method=method.lower(),
            host=unquote(rt.path),
            params=urlencode(uri_params),
            headers='&'.join(map(lambda p: (lambda x, y: "%s=%s" % (x, y))(*p), sorted(headers.items())))
         )
        logger.debug("format str: " + format_str)

        start_sign_time = int(time.time())
        sign_time = "{bg_time};{ed_time}".format(bg_time=start_sign_time-60, ed_time=start_sign_time + self._expire)
        sha1 = hashlib.sha1()
        sha1.update(to_bytes(format_str))

        str_to_sign = "sha1\n{time}\n{sha1}\n".format(time=sign_time, sha1=sha1.hexdigest())
        logger.debug('str_to_sign: ' + str(str_to_sign))

        sign_key = hmac.new(to_bytes(self._secret_key), to_bytes(sign_time), hashlib.sha1).hexdigest()
        sign = hmac.new(to_bytes(sign_key), to_bytes(str_to_sign), hashlib.sha1).hexdigest()
        logger.debug('sign_key: ' + str(sign_key))
        logger.debug('sign: ' + str(sign))
        sign_tpl = "q-sign-algorithm=sha1&q-ak={ak}&q-sign-time={sign_time}&q-key-time={key_time}&q-header-list={headers}&q-url-param-list={params}&q-signature={sign}"
        r.headers = tmp_r
        r.headers['Authorization'] = sign_tpl.format(
            ak=self._access_id,
            sign_time=sign_time,
            key_time=sign_time,
            params=';'.join(sorted(map(lambda k: k.lower(), uri_params.keys()))),
            headers=';'.join(sorted(headers.keys())),
            sign=sign
        )
        if self._anonymous:
            r.headers['Authorization'] = ""
        r.headers['User-agent'] = 'coscmd-v' + Version
        logger.debug("sign_key" + str(sign_key))
        logger.debug(r.headers['Authorization'])

        logger.debug("request headers: " + str(r.headers))
        return r


if __name__ == "__main__":
    url = 'http://Lewzylu01-1252448703.cn-south.myqcloud.com/a.txt'
    logger.debug("init with : " + url)
    request = requests.session()
    secret_id = 'AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00'
    secret_key = 'ciivKvnnrMvSvQpMAWuIz12pThGGlWRW'
    rt = request.get(url=url+"", auth=CosS3Auth(secret_id, secret_key))
