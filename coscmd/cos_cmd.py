# -*- coding: utf-8 -*-
from six.moves.configparser import SafeConfigParser
from six import text_type
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
import sys
import logging
import os
import json
import requests
import qcloud_cos
from threading import Thread
from coscmd import cos_global

if sys.version > '3':
    from coscmd.cos_client import CoscmdConfig, CosS3Client
    from coscmd.cos_global import Version
else:
    from cos_client import CoscmdConfig, CosS3Client
    from cos_global import Version

logger = logging.getLogger("coscmd")

fs_coding = sys.getfilesystemencoding()

pre_appid = ""
pre_bucket = ""
config_path = ""
silence = False
global res


def concat_path(sorce_path, target_path):
    sorce_path = sorce_path.replace('\\', '/')
    target_path = target_path.replace('\\', '/')
    if sorce_path.endswith('/') is False:
        if target_path.endswith('/') is True:
            target_path += sorce_path.split('/')[-1]
    return sorce_path, target_path


def to_printable_str(s):
    if isinstance(s, text_type):
        return s.encode(fs_coding)
    else:
        return s


def config(args):
    logger.debug("config: " + str(args))

    conf_path = os.path.expanduser(config_path)

    with open(conf_path, 'w+') as f:
        cp = SafeConfigParser()
        cp.add_section("common")
        cp.set('common', 'secret_id', args.secret_id)
        cp.set('common', 'secret_key', args.secret_key)
        if args.token != "":
            cp.set('common', 'token', args.token)
        cp.set('common', 'bucket', args.bucket)
        if args.endpoint:
            cp.set('common', 'endpoint', args.endpoint)
        else:
            cp.set('common', 'region', args.region)
        cp.set('common', 'max_thread', str(args.max_thread))
        cp.set('common', 'part_size', str(args.part_size))
        cp.set('common', 'retry', str(args.retry))
        cp.set('common', 'timeout', str(args.timeout))
        if args.appid != "":
            cp.set('common', 'appid', args.appid)
        if args.use_http:
            cp.set('common', 'schema', 'http')
        else:
            cp.set('common', 'schema', 'https')
        cp.set('common', 'verify', args.verify)
        if args.anonymous:
            cp.set('common', 'anonymous', 'True')
        else:
            cp.set('common', 'anonymous', 'False')
        cp.write(f)
        logger.info("Created configuration file in {path}".format(path=to_printable_str(conf_path)))


def compatible(region):
    if region is None:
        return None
    _dict = {'tj': 'ap-beijing-1', 'bj': 'ap-beijing', 'gz': 'ap-guangzhou', 'sh': 'ap-shanghai',
             'cd': 'ap-chengdu', 'spg': 'ap-singapore', 'hk': 'ap-hongkong', 'ca': 'na-toronto', 'ger': 'eu-frankfurt',
             'cn-south': 'ap-guangzhou', 'cn-north': 'ap-beijing-1'}
    if region.startswith('cos.'):
        region = region[4:]
    if region in _dict:
        region = _dict[region]
    return region


def load_conf():

    conf_path = os.path.expanduser(config_path)
    if not os.path.exists(conf_path):
        logger.warn("{conf} couldn't be found, please use \'coscmd config -h\' to learn how to config coscmd!".format(conf=to_printable_str(conf_path)))
        raise IOError
    else:
        logger.debug('{conf} is found'.format(conf=to_printable_str(conf_path)))

    try:
        with open(conf_path, 'r') as f:
            cp = SafeConfigParser()
            cp.readfp(fp=f)
            if not cp.has_section('common'):
                raise Exception("[common] section could't be found, please check your config file.")
            if cp.has_option('common', 'part_size'):
                part_size = cp.getint('common', 'part_size')
            else:
                part_size = 1
            if cp.has_option('common', 'max_thread'):
                max_thread = cp.getint('common', 'max_thread')
            else:
                max_thread = 5
            try:
                secret_id = cp.get('common', 'secret_id')
            except Exception:
                secret_id = cp.get('common', 'access_id')
            try:
                appid = cp.get('common', 'appid')
                bucket = cp.get('common', 'bucket')
                if bucket.endswith("-"+str(appid)):
                    bucket = bucket.rstrip(appid)
                    bucket = bucket[:-1]
            except Exception:
                try:
                    bucket = cp.get('common', 'bucket')
                    appid = bucket.split('-')[-1]
                    bucket = bucket.rstrip(appid)
                    bucket = bucket[:-1]
                except Exception:
                    # check if user use -b bucket
                    if (pre_bucket == ""):
                        logger.error("The configuration file is wrong. Please reconfirm")
            try:
                schema = cp.get('common', 'schema')
            except Exception:
                schema = 'https'
            try:
                verify = cp.get('common', 'verify')
            except Exception:
                verify = 'md5'
            try:
                token = cp.get('common', 'token')
            except Exception:
                token = None
            try:
                anonymous = cp.get('common', 'anonymous')
                if anonymous == 'True' or anonymous == 'true':
                    anonymous = True
                else:
                    anonymous = False
            except Exception:
                anonymous = False
            try:
                retry = int(cp.get('common', 'retry'))
            except Exception:
                retry = 2
            try:
                timeout = int(cp.get('common', 'timeout'))
            except Exception:
                timeout = 60
            
            try:
                multiupload_threshold = int(cp.get('common', 'multiupload_threshold'))
            except Exception:
                multiupload_threshold = 100
            if multiupload_threshold < 20:
                multiupload_threshold = 20
            try:
                multidownload_threshold = int(cp.get('common', 'multidownload_threshold'))
            except Exception:
                multidownload_threshold = 100
            if multidownload_threshold < 20:
                multidownload_threshold = 20
            region, endpoint = None, None
            if cp.has_option('common', 'region'):
                region = cp.get('common', 'region')
            if cp.has_option('common', 'endpoint'):
                endpoint = cp.get('common', 'endpoint')
            if pre_appid != "":
                appid = pre_appid
            if pre_bucket != "":
                bucket = pre_bucket
            if pre_region != "":
                region = pre_region
            conf = CoscmdConfig(
                appid=appid,
                secret_id=secret_id,
                secret_key=cp.get('common', 'secret_key'),
                token=token,
                region=compatible(region),
                endpoint=endpoint,
                bucket=bucket,
                part_size=part_size,
                max_thread=max_thread,
                schema=schema,
                anonymous=anonymous,
                verify=verify,
                retry=retry,
                timeout=timeout,
                silence=silence,
                multiupload_threshold=multiupload_threshold,
                multidownload_threshold=multidownload_threshold
            )
            return conf
    except Exception as e:
        raise(e)


class Op(object):
    @staticmethod
    def upload(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if args.cos_path == "":
                args.cos_path = "/"
            Interface = client.op_int()

            if not isinstance(args.local_path, text_type):
                args.local_path = args.local_path.decode(fs_coding)
            if not isinstance(args.cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)

            if not os.path.exists(args.local_path):
                logger.warn("cannot stat '%s': No such file or directory" % to_printable_str(args.local_path))
                return -1

            if not os.access(args.local_path, os.R_OK):
                logger.warn('local_path %s is not readable!' % to_printable_str(args.local_path))
                return -1
            args.local_path, args.cos_path = concat_path(args.local_path, args.cos_path)
            if args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            kwargs = {}
            kwargs['sync'] = args.sync
            kwargs['skipmd5'] = args.skipmd5
            kwargs['ignore'] = args.ignore.split(',')
            kwargs['include'] = args.include.split(',')
            kwargs['force'] = args.force
            kwargs['delete'] = args.delete
            kwargs['yes'] = args.yes
            if args.recursive:
                if os.path.isfile(args.local_path) is True:
                    rt = Interface.upload_file(args.local_path, args.cos_path, args.headers, **kwargs)
                    return rt
                elif os.path.isdir(args.local_path):
                    rt = Interface.upload_folder(args.local_path, args.cos_path, args.headers, **kwargs)
                    return rt
            else:
                if os.path.isdir(args.local_path):
                    logger.warn("\"{path}\" is a directory, use \'-r\' option to upload it please".format(path=to_printable_str(args.local_path)))
                    return -1
                if os.path.isfile(args.local_path) is False:
                    logger.warn("cannot stat '%s': No such file or directory" % to_printable_str(args.local_path))
                    return -1
                rt = Interface.upload_file(args.local_path, args.cos_path, args.headers, **kwargs)
                return rt
            return -1
        except Exception as e:
            logger.wran(e)
            return -2

    @staticmethod
    def download(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            if not isinstance(args.local_path, text_type):
                args.local_path = args.local_path.decode(fs_coding)
            if not isinstance(args.cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            args.cos_path, args.local_path = concat_path(args.cos_path, args.local_path)
            if args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            kwargs = {}
            kwargs['force'] = args.force
            kwargs['sync'] = args.sync
            kwargs['num'] = min(20, args.num)
            kwargs['ignore'] = args.ignore.split(',')
            kwargs['include'] = args.include.split(',')
            kwargs['skipmd5'] = args.skipmd5
            kwargs['delete'] = args.delete
            kwargs['yes'] = args.yes
            if args.recursive:
                rt = Interface.download_folder(args.cos_path, args.local_path, args.headers, **kwargs)
                return rt
            else:
                rt = Interface.download_file(args.cos_path, args.local_path, args.headers, **kwargs)
                return rt
            return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def delete(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            Interface = client.op_int()

            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)

            kwargs = {}
            kwargs['force'] = args.force
            kwargs['versions'] = args.versions
            kwargs['versionId'] = args.versionId
            kwargs['yes'] = args.yes
            if args.recursive:
                if args.cos_path.endswith('/') is False:
                    args.cos_path += '/'
                if args.cos_path == '/':
                    args.cos_path = ''
                if not Interface.delete_folder(args.cos_path, **kwargs):
                    logger.debug("delete all files under {cos_path} successfully!".format(cos_path=to_printable_str(args.cos_path)))
                    return 0
                else:
                    logger.debug("delete all files under {cos_path} failed!".format(cos_path=to_printable_str(args.cos_path)))
                    return -1
            else:
                if args.cos_path == '':
                    logger.warn("not support delete empty path")
                    return -1
                if not Interface.delete_file(args.cos_path, **kwargs):
                    logger.debug("delete all files under {cos_path} successfully!".format(cos_path=to_printable_str(args.cos_path)))
                    return 0
                else:
                    logger.debug("delete all files under {cos_path} failed!".format(cos_path=to_printable_str(args.cos_path)))
                    return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def copy(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            _, args.cos_path = concat_path(args.source_path, args.cos_path)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if not isinstance(args.source_path, text_type):
                args.source_path = args.source_path.decode(fs_coding)
            if not isinstance(args.cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)

            kwargs = {}
            kwargs['sync'] = args.sync
            kwargs['force'] = args.force
            kwargs['directive'] = args.directive
            kwargs['skipmd5'] = args.skipmd5
            kwargs['ignore'] = args.ignore.split(',')
            kwargs['include'] = args.include.split(',')
            kwargs['delete'] = args.delete
            kwargs['yes'] = args.yes
            kwargs['move'] = False
            if args.recursive:
                _, args.cos_path = concat_path(args.source_path, args.cos_path)
                if args.cos_path.endswith('/') is False:
                    args.cos_path += '/'
                if args.cos_path.startswith('/'):
                    args.cos_path = args.cos_path[1:]
                if not Interface.copy_folder(args.source_path, args.cos_path, args.headers, **kwargs):
                    return 0
                else:
                    return 1
            else:
                if not Interface.copy_file(args.source_path, args.cos_path, args.headers, **kwargs):
                    return 0
                else:
                    return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def move(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            _, args.cos_path = concat_path(args.source_path, args.cos_path)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if not isinstance(args.source_path, text_type):
                args.source_path = args.source_path.decode(fs_coding)
            if not isinstance(args.cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)

            kwargs = {}
            kwargs['sync'] = False
            kwargs['force'] = True
            kwargs['directive'] = args.directive
            kwargs['skipmd5'] = True
            kwargs['ignore'] = args.ignore.split(',')
            kwargs['include'] = args.include.split(',')
            kwargs['delete'] = False
            kwargs['move'] = True
            if args.recursive:
                _, args.cos_path = concat_path(args.source_path, args.cos_path)
                if args.cos_path.endswith('/') is False:
                    args.cos_path += '/'
                if args.cos_path.startswith('/'):
                    args.cos_path = args.cos_path[1:]
                if not Interface.copy_folder(args.source_path, args.cos_path, args.headers, **kwargs):
                    return 0
                else:
                    return 1
            else:
                if not Interface.copy_file(args.source_path, args.cos_path, args.headers, **kwargs):
                    return 0
                else:
                    return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def list(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()
            kwargs = {}
            kwargs['recursive'] = args.recursive
            kwargs['all'] = args.all
            kwargs['num'] = args.num
            kwargs['human'] = args.human
            kwargs['versions'] = args.versions
            if not Interface.list_objects(cos_path=args.cos_path, **kwargs):
                return 0
            else:
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def list_parts(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            if Interface.list_multipart_uploads(cos_path=args.cos_path):
                return 0
            else:
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def abort(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]

            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()
            if not Interface.abort_parts(cos_path=args.cos_path):
                return 0
            else:
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def info(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]

            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()
            if not Interface.info_object(args.cos_path, _human=args.human):
                return 0
            else:
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def restore(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]

            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()
            kwargs = {}
            kwargs['day'] = args.day
            kwargs['tier'] = args.tier
            if args.recursive:
                if not Interface.restore_folder(cos_path=args.cos_path, **kwargs):
                    return 0
                else:
                    return -1
            else:
                if not Interface.restore_file(cos_path=args.cos_path, **kwargs):
                    return 0
                else:
                    return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def signurl(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            if not isinstance(args.cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            try:
                Interface = client.op_int()
                rt = Interface.sign_url(args.cos_path, args.timeout)
                if rt:
                    return 0
                else:
                    return -1
            except Exception:
                logger.warn('Geturl fail')
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def put_object_acl(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()
            rt = Interface.put_object_acl(args.grant_read, args.grant_write, args.grant_full_control, args.cos_path)
            if rt is True:
                return 0
            else:
                logger.warn("Put object acl fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def get_object_acl(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            while args.cos_path.startswith('/'):
                args.cos_path = args.cos_path[1:]
            if not isinstance(args. cos_path, text_type):
                args.cos_path = args.cos_path.decode(fs_coding)
            Interface = client.op_int()

            rt = Interface.get_object_acl(args.cos_path)
            if rt is True:
                return 0
            else:
                logger.warn("Get object acl fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def create_bucket(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            if Interface.create_bucket():
                return 0
            else:
                logger.warn("Create bucket fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def delete_bucket(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            kwargs = {}
            kwargs['force'] = args.force
            if Interface.delete_bucket(**kwargs):
                return 0
            else:
                logger.warn("Delete bucket fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def put_bucket_acl(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            rt = Interface.put_bucket_acl(args.grant_read, args.grant_write, args.grant_full_control)
            if rt is True:
                return 0
            else:
                logger.warn("put bucket acl fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def get_bucket_acl(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            rt = Interface.get_bucket_acl()
            if rt is True:
                return 0
            else:
                logger.warn("Get bucket acl fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def put_bucket_versioning(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            rt = Interface.put_bucket_versioning(args.status)
            if rt is True:
                return 0
            else:
                logger.warn("Put bucket versioning fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def get_bucket_versioning(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            rt = Interface.get_bucket_versioning()
            if rt is True:
                return 0
            else:
                logger.warn("Get bucket versioning fail")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2

    @staticmethod
    def probe(args):
        try:
            conf = load_conf()
            client = CosS3Client(conf)
            Interface = client.op_int()
            kwargs = {}
            kwargs['test_num'] = args.num
            kwargs['file_size'] = args.size
            rt = Interface.probe(**kwargs)
            if 0 == rt:
                return 0
            else:
                logger.warn("probe failed")
                return -1
        except Exception as e:
            logger.warn(e)
            return -2


def get_version():
    logger.info(Version)
    return 0


def version_check():
    try:
        ret = requests.get("https://pypi.org/pypi/coscmd/json").content
        res_json = json.loads(ret)
        latest_version = res_json["info"]["version"]
        lat_spl = latest_version.split('.')
        cur_spl = cos_global.Version.split('.')
        if cur_spl[0] < lat_spl[0] or cur_spl[1] < lat_spl[1] or cur_spl[2] < lat_spl[2]:
            logger.info("The current version of coscmd is {v1} \
and the latest version is {v2}. It is recommended \
to upgrade coscmd with the command'pip install coscmd -U'.".format(v1=cos_global.Version, v2=latest_version))
    except Exception as e:
        logger.debug(e)


def command_thread():
    global res
    res = -1
    desc = """an easy-to-use but powerful command-line tool.
              try \'coscmd -h\' to get more informations.
              try \'coscmd sub-command -h\' to learn all command usage, likes \'coscmd upload -h\'"""
    parser = ArgumentParser(description=desc)
    parser.add_argument('-d', '--debug', help="Debug mode", action="store_true", default=False)
    parser.add_argument('-s', '--silence', help="Silence mode", action="store_true", default=False)
    parser.add_argument('-b', '--bucket', help="Specify bucket", type=str, default="")
    parser.add_argument('-r', '--region', help="Specify region", type=str, default="")
    parser.add_argument('-c', '--config_path', help="Specify config_path", type=str, default="~/.cos.conf")
    parser.add_argument('-l', '--log_path', help="Specify log_path", type=str, default="~/.cos.log")
    parser.add_argument('--log_size', help='specify max log size in MB (default 1MB)', type=int, default=128)
    parser.add_argument('--log_backup_count', help='specify log backup num', type=int, default=1)

    sub_parser = parser.add_subparsers()
    parser_config = sub_parser.add_parser("config", help="Config your information at first")
    parser_config.add_argument('-a', '--secret_id', help='Specify your secret id', type=str, required=True)
    parser_config.add_argument('-s', '--secret_key', help='Specify your secret key', type=str, required=True)
    parser_config.add_argument('-t', '--token', help='Set x-cos-security-token header', type=str, default="")
    parser_config.add_argument('-b', '--bucket', help='Specify your bucket', type=str, required=True)

    group = parser_config.add_mutually_exclusive_group(required=True)
    group.add_argument('-r', '--region', help='Specify your region', type=str)
    group.add_argument('-e', '--endpoint', help='Specify COS endpoint', type=str)

    parser_config.add_argument('-m', '--max_thread', help='Specify the number of threads (default 5)', type=int, default=5)
    parser_config.add_argument('-p', '--part_size', help='specify min part size in MB (default 1MB)', type=int, default=1)
    parser_config.add_argument('--retry', help='specify retry times', type=int, default=5)
    parser_config.add_argument('--timeout', help='specify request timeout', type=int, default=60)
    parser_config.add_argument('-u', '--appid', help='Specify your appid', type=str, default="")
    parser_config.add_argument('--verify', help='Specify your encryption method', type=str, default="md5")
    parser_config.add_argument('--do-not-use-ssl', help="Use http://", action="store_true", default=False, dest="use_http")
    parser_config.add_argument('--anonymous', help="Anonymous operation", action="store_true", default=False, dest="anonymous")
    parser_config.set_defaults(func=config)

    parser_upload = sub_parser.add_parser("upload", help="Upload file or directory to COS")
    parser_upload.add_argument('local_path', help="Local file path as /tmp/a.txt or directory", type=str)
    parser_upload.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_upload.add_argument('-r', '--recursive', help="Upload recursively when upload directory", action="store_true", default=False)
    parser_upload.add_argument('-H', '--headers', help="Specify HTTP headers", type=str, default='{}')
    parser_upload.add_argument('-s', '--sync', help="Upload and skip the same file", action="store_true", default=False)
    parser_upload.add_argument('-f', '--force', help="upload without history breakpoint", action="store_true", default=False)
    parser_upload.add_argument('-y', '--yes', help="Skip confirmation", action="store_true", default=False)
    parser_upload.add_argument('--include', help='Specify filter rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="*")
    parser_upload.add_argument('--ignore', help='Specify ignored rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="")
    parser_upload.add_argument('--skipmd5', help='Upload without x-cos-meta-md5 / sync without check md5, only check filename and filesize', action="store_true", default=False)
    parser_upload.add_argument('--delete', help="delete objects which exists in cos but not exist in local", action="store_true", default=False)
    parser_upload.set_defaults(func=Op.upload)

    parser_download = sub_parser.add_parser("download", help="Download file from COS to local")
    parser_download.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_download.add_argument('local_path', help="Local file path as /tmp/a.txt", type=str)
    parser_download.add_argument('-f', '--force', help="Overwrite the saved files", action="store_true", default=False)
    parser_download.add_argument('-y', '--yes', help="Skip confirmation", action="store_true", default=False)
    parser_download.add_argument('-r', '--recursive', help="Download recursively when upload directory", action="store_true", default=False)
    parser_download.add_argument('-s', '--sync', help="Download and skip the same file", action="store_true", default=False)
    parser_download.add_argument('-H', '--headers', help="Specify HTTP headers", type=str, default='{}')
    parser_download.add_argument('--versionId', help='Specify versionId of object to list', type=str, default="")
    parser_download.add_argument('--include', help='Specify filter rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="*")
    parser_download.add_argument('--ignore', help='Specify ignored rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="")
    parser_download.add_argument('--skipmd5', help='Download sync without check md5, only check filename and filesize', action="store_true", default=False)
    parser_download.add_argument('--delete', help="delete objects which exists in local but not exist in cos", action="store_true", default=False)
    parser_download.add_argument('-n', '--num', help='Specify max part_num of multidownload', type=int, default=10)
    parser_download.set_defaults(func=Op.download)

    parser_delete = sub_parser.add_parser("delete", help="Delete file or files on COS")
    parser_delete.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_delete.add_argument('-r', '--recursive', help="Delete files recursively, WARN: all files with the prefix will be deleted!", action="store_true", default=False)
    parser_delete.add_argument('--versions', help='Delete objects with versions', action="store_true", default=False)
    parser_delete.add_argument('--versionId', help='Specify versionId of object to list', type=str, default="")
    parser_delete.add_argument('-f', '--force', help="Delete directly without confirmation", action="store_true", default=False)
    parser_delete.add_argument('-y', '--yes', help="Delete directly without confirmation", action="store_true", default=False)
    parser_delete.set_defaults(func=Op.delete)

    parser_abort = sub_parser.add_parser("abort", help='Aborts upload parts on COS')
    parser_abort.add_argument("cos_path", nargs='?', help="Cos_path as a/b.txt", type=str, default='')
    parser_abort.set_defaults(func=Op.abort)

    parser_copy = sub_parser.add_parser("copy", help="Copy file from COS to COS")
    parser_copy.add_argument('source_path', help="Source file path as 'bucket-appid.cos.ap-guangzhou.myqcloud.com/a.txt'", type=str)
    parser_copy.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_copy.add_argument('-H', '--headers', help="Specify HTTP headers", type=str, default='{}')
    parser_copy.add_argument('-d', '--directive', help="if Overwrite headers", type=str, choices=['Copy', 'Replaced'], default="Copy")
    parser_copy.add_argument('-s', '--sync', help="Copy and skip the same file", action="store_true", default=False)
    parser_copy.add_argument('-r', '--recursive', help="Copy files recursively", action="store_true", default=False)
    parser_copy.add_argument('-f', '--force', help="Overwrite file without skip", action="store_true", default=False)
    parser_copy.add_argument('-y', '--yes', help="Skip confirmation", action="store_true", default=False)
    parser_copy.add_argument('--include', help='Specify filter rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="*")
    parser_copy.add_argument('--ignore', help='Specify ignored rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="")
    parser_copy.add_argument('--skipmd5', help='Copy sync without check md5, only check filename and filesize', action="store_true", default=False)
    parser_copy.add_argument('--delete', help="delete objects which exists in sourcepath but not exist in dstpath", action="store_true", default=False)
    parser_copy.set_defaults(func=Op.copy)

    parser_move = sub_parser.add_parser("move", help="move file from COS to COS")
    parser_move.add_argument('source_path', help="Source file path as 'bucket-appid.cos.ap-guangzhou.myqcloud.com/a.txt'", type=str)
    parser_move.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_move.add_argument('-H', '--headers', help="Specify HTTP headers", type=str, default='{}')
    parser_move.add_argument('-d', '--directive', help="if Overwrite headers", type=str, choices=['Copy', 'Replaced'], default="Copy")
    parser_move.add_argument('-r', '--recursive', help="Copy files recursively", action="store_true", default=False)
    parser_move.add_argument('--include', help='Specify filter rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="*")
    parser_move.add_argument('--ignore', help='Specify ignored rules, separated by commas; Example: *.txt,*.docx,*.ppt', type=str, default="")
    parser_move.set_defaults(func=Op.move)

    parser_list = sub_parser.add_parser("list", help='List files on COS')
    parser_list.add_argument("cos_path", nargs='?', help="Cos_path as a/b.txt", type=str, default='')
    parser_list.add_argument('-a', '--all', help="List all the files", action="store_true", default=False)
    parser_list.add_argument('-r', '--recursive', help="List files recursively", action="store_true", default=False)
    parser_list.add_argument('-n', '--num', help='Specify max num of files to list', type=int, default=100)
    parser_list.add_argument('-v', '--versions', help='List object with versions', action="store_true", default=False)
    parser_list.add_argument('--human', help='Humanized display', action="store_true", default=False)
    parser_list.set_defaults(func=Op.list)

    parser_list_parts = sub_parser.add_parser("listparts", help="List upload parts")
    parser_list_parts.add_argument("cos_path", nargs='?', help="Cos_path as a/b.txt", type=str, default='')
    parser_list_parts.set_defaults(func=Op.list_parts)

    parser_info = sub_parser.add_parser("info", help="Get the information of file on COS")
    parser_info.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_info.add_argument('--human', help='Humanized display', action="store_true", default=False)
    parser_info.set_defaults(func=Op.info)

    parser_restore = sub_parser.add_parser("restore", help="Restore")
    parser_restore.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_restore.add_argument('-r', '--recursive', help="Restore files recursively", action="store_true", default=False)
    parser_restore.add_argument('-d', '--day', help='Specify lifetime of the restored (active) copy', type=int, default=7)
    parser_restore.add_argument('-t', '--tier', help='Specify the data access tier', type=str, choices=['Expedited', 'Standard', 'Bulk'], default='Standard')
    parser_restore.set_defaults(func=Op.restore)

    parser_signurl = sub_parser.add_parser("signurl", help="Get download url")
    parser_signurl.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_signurl.add_argument('-t', '--timeout', help='Specify the signature valid time', type=int, default=10000)
    parser_signurl.set_defaults(func=Op.signurl)

    parser_create_bucket = sub_parser.add_parser("createbucket", help='Create bucket')
    parser_create_bucket.set_defaults(func=Op.create_bucket)

    parser_delete_bucket = sub_parser.add_parser("deletebucket", help='Delete bucket')
    parser_delete_bucket.add_argument('-f', '--force', help="Clear all inside the bucket and delete bucket", action="store_true", default=False)
    parser_delete_bucket.set_defaults(func=Op.delete_bucket)

    parser_put_object_acl = sub_parser.add_parser("putobjectacl", help='''Set object acl''')
    parser_put_object_acl.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_put_object_acl.add_argument('--grant-read', dest='grant_read', help='Set grant-read', type=str, required=False)
    parser_put_object_acl.add_argument('--grant-write', dest='grant_write', help='Set grant-write', type=str, required=False)
    parser_put_object_acl.add_argument('--grant-full-control', dest='grant_full_control', help='Set grant-full-control', type=str, required=False)
    parser_put_object_acl.set_defaults(func=Op.put_object_acl)

    parser_get_object_acl = sub_parser.add_parser("getobjectacl", help='Get object acl')
    parser_get_object_acl.add_argument("cos_path", help="Cos_path as a/b.txt", type=str)
    parser_get_object_acl.set_defaults(func=Op.get_object_acl)

    parser_put_bucket_acl = sub_parser.add_parser("putbucketacl", help='''Set bucket acl''')
    parser_put_bucket_acl.add_argument('--grant-read', dest='grant_read', help='Set grant-read', type=str, required=False)
    parser_put_bucket_acl.add_argument('--grant-write', dest='grant_write', help='Set grant-write', type=str, required=False)
    parser_put_bucket_acl.add_argument('--grant-full-control', dest='grant_full_control', help='Set grant-full-control', type=str, required=False)
    parser_put_bucket_acl.set_defaults(func=Op.put_bucket_acl)

    parser_get_bucket_acl = sub_parser.add_parser("getbucketacl", help='Get bucket acl')
    parser_get_bucket_acl.set_defaults(func=Op.get_bucket_acl)

    parser_put_bucket_versioning = sub_parser.add_parser("putbucketversioning", help="Set the versioning state")
    parser_put_bucket_versioning.add_argument("status",  help="Status as a/b.txt", type=str, choices=['Enabled', 'Suspended'], default='Enable')
    parser_put_bucket_versioning.set_defaults(func=Op.put_bucket_versioning)

    parser_get_bucket_versioning = sub_parser.add_parser("getbucketversioning", help="Get the versioning state")
    parser_get_bucket_versioning.set_defaults(func=Op.get_bucket_versioning)

    parser_probe = sub_parser.add_parser("probe", help="Connection test")
    parser_probe.add_argument('-n', '--num', help='Specify test times', type=int, default=3)
    parser_probe.add_argument('-s', '--size', help='Specify test filesize(unit MB)', type=int, default=1)
    parser_probe.set_defaults(func=Op.probe)

    parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + Version)

    try:
        args = parser.parse_args()
    except SystemExit as exc:
        res = exc.code
        return 0

    logger = logging.getLogger('coscmd')
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger_qcloud_cos = logging.getLogger('qcloud_cos')
    logger_qcloud_cos.setLevel(logging.ERROR)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        console.setLevel(logging.DEBUG)
        logger_qcloud_cos.setLevel(logging.DEBUG)
    if args.silence:
        logger.setLevel(logging.FATAL)
        logger_qcloud_cos.setLevel(logging.FATAL)
        console.setLevel(logging.INFO)
    handler = RotatingFileHandler(os.path.expanduser(args.log_path), maxBytes=args.log_size*1024*1024, backupCount=args.log_backup_count)
    handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s]:  %(message)s'))
    logger.addHandler(handler)
    logger_qcloud_cos.addHandler(handler)
    logger.addHandler(console)
    logger_qcloud_cos.addHandler(console)
    global pre_appid, pre_bucket, pre_region, config_path, silence
    config_path = args.config_path
    pre_bucket = args.bucket
    pre_region = args.region
    silence = args.silence
    try:
        pre_appid = pre_bucket.split('-')[-1]
        pre_bucket = pre_bucket.rstrip(pre_appid)
        pre_bucket = pre_bucket[:-1]
    except Exception:
        logger.warn("set bucket error")
    try:
        res = args.func(args)
        return res
    except Exception:
        return 0


def main_thread():
    mainthread = Thread()
    mainthread.start()
    thread_ = Thread(target=command_thread)
    thread_.start()
    import time
    try:
        while True:
            time.sleep(1)
            if thread_.is_alive() is False:
                break
    except KeyboardInterrupt:
        mainthread.stop()
        thread_.stop()
        sys.exit()


def _main():
    thread_ = Thread(target=main_thread)
    thread_.daemon = True
    thread_.start()
    try:
        while thread_.is_alive():
            thread_.join(2)
    except KeyboardInterrupt:
        logger.info('exiting')
        return 1
    global res
    return res


if __name__ == '__main__':
    _main()
    global res
    sys.exit(res)
