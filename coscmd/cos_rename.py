# -*- coding=utf-8
import json
import threading

from qcloud_cos import CosServiceError, CosClientError, CosS3Client
from qcloud_cos.cos_comm import check_object_content_length, get_content_md5, client_can_retry, format_values
from requests import Request, Timeout, __version__

from .cos_auth import CosS3Auth
from .cos_comm import *

# python 3.10报错"module 'collections' has no attribute 'Iterable'"，这里先规避
if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
    import collections.abc
    collections.Iterable = collections.abc.Iterable

logger = logging.getLogger(__name__)


class CosMoveConfig(object):
    __built_in_sessions = None  # 内置的静态连接池，多个Client间共享使用

    def __init__(self, conf, retry=1, session=None):
        """初始化client对象

        :param conf(CosConfig): 用户的配置.
        :param retry(int): 失败重试的次数.
        :param session(object): http session.
        """
        self._conf = conf
        self._retry = retry  # 重试的次数，分片上传时可适当增大

        if not CosMoveConfig.__built_in_sessions:
            with threading.Lock():
                if not CosMoveConfig.__built_in_sessions:  # 加锁后double check
                    CosMoveConfig.__built_in_sessions = self.generate_built_in_connection_pool(
                        self._conf._pool_connections, self._conf._pool_maxsize)

        if session is None:
            self._session = CosMoveConfig.__built_in_sessions
        else:
            self._session = session

    def set_built_in_connection_pool_max_size(self, PoolConnections, PoolMaxSize):
        """设置SDK内置的连接池的连接大小，并且重新绑定到client中"""
        if not CosS3Client.__built_in_sessions:
            return

        if CosS3Client.__built_in_sessions.get_adapter('http://')._pool_connections == PoolConnections \
           and CosS3Client.__built_in_sessions.get_adapter('http://')._pool_maxsize == PoolMaxSize:
            return

        # 判断之前是否绑定到内置连接池
        rebound = False
        if self._session and self._session is CosS3Client.__built_in_sessions:
            rebound = True

        # 重新生成内置连接池
        CosS3Client.__built_in_sessions.close()
        CosS3Client.__built_in_sessions = self.generate_built_in_connection_pool(PoolConnections, PoolMaxSize)

        # 重新绑定到内置连接池
        if rebound:
            self._session = CosS3Client.__built_in_sessions
            logger.info("rebound built-in connection pool success. maxsize=%d,%d" % (PoolConnections, PoolMaxSize))

    def generate_built_in_connection_pool(self, PoolConnections, PoolMaxSize):
        """生成SDK内置的连接池，此连接池是client间共用的"""
        built_in_sessions = requests.session()
        built_in_sessions.mount('http://', requests.adapters.HTTPAdapter(pool_connections=PoolConnections, pool_maxsize=PoolMaxSize))
        built_in_sessions.mount('https://', requests.adapters.HTTPAdapter(pool_connections=PoolConnections, pool_maxsize=PoolMaxSize))
        logger.info("generate built-in connection pool success. maxsize=%d,%d" % (PoolConnections, PoolMaxSize))
        return built_in_sessions

    def get_conf(self):
        """获取配置"""
        return self._conf

    def get_auth(self, Method, Bucket, Key, Expired=300, Headers={}, Params={}, SignHost=None):
        """获取签名

        :param Method(string): http method,如'PUT','GET'.
        :param Bucket(string): 存储桶名称.
        :param Key(string): 请求COS的路径.
        :param Expired(int): 签名有效时间,单位为s.
        :param headers(dict): 签名中的http headers.
        :param params(dict): 签名中的http params.
        :param SignHost(bool): 是否将host算入签名.
        :return (string): 计算出的V5签名.

        .. code-block:: python

            config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取上传请求的签名
            auth_string = client.get_auth(
                    Method='PUT',
                    Bucket='bucket',
                    Key='test.txt',
                    Expired=600,
                    Headers={'header1': 'value1'},
                    Params={'param1': 'value1'}
                )
            print (auth_string)
        """

        # python中默认参数只会初始化一次，这里重新生成可变对象实例避免多线程访问问题
        if not Headers:
            Headers = dict()
        if not Params:
            Params = dict()

        url = self._conf.uri(bucket=Bucket, path=Key)
        r = Request(Method, url, headers=Headers, params=Params)
        auth = CosS3Auth(self._conf, Key, Params, Expired, SignHost)
        return auth(r).headers['Authorization']

    def move_object(self, source_path, url, _http_headers='{}', EnableMD5=False):
        http_headers = _http_headers
        logger.info("put object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=http_headers))
        if EnableMD5:
            md5_str = get_content_md5(url)
            if md5_str:
                http_headers['Content-MD5'] = md5_str
        http_headers['x-cos-rename-source'] = source_path
        logger.info("put object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=http_headers))
        rt = self._session.put(url=url, auth=CosS3Auth(self._conf), headers=http_headers, timeout=self._timeout)
        if rt.status_code == 200:
            return 0
        else:
            raise Exception(response_info(rt))

if __name__ == "__main__":
    pass