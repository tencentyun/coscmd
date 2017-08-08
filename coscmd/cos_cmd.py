# -*- coding: utf-8 -*-
from cos_client import CosConfig, CosS3Client
from ConfigParser import SafeConfigParser
from argparse import ArgumentParser
import sys
import logging
import coloredlogs
import os

logger = logging.getLogger(__name__)

fs_coding = sys.getfilesystemencoding()

color_red = "31"
color_green = "32"
color_yello = "33"


def to_printable_str(s):
    if isinstance(s, unicode):
        return s.encode(fs_coding)
    else:
        return s


def change_color(s, color):
    return "\033[1;" + color + ";40m" + s + "\033[0m"


def config(args):
    logger.debug("config: " + str(args))

    conf_path = os.path.expanduser('~/.cos.conf')

    with open(conf_path, 'w+') as f:
        cp = SafeConfigParser()
        cp.add_section("common")
        cp.set('common', 'access_id', args.access_id)
        cp.set('common', 'secret_key', args.secret_key)
        cp.set('common', 'appid', args.appid)
        cp.set('common', 'bucket', args.bucket)
        cp.set('common', 'region', args.region)
        cp.set('common', 'max_thread', str(args.max_thread))
        cp.set('common', 'part_size', str(args.part_size))
        cp.write(f)
        logger.info("Created configuration file in {path}".format(path=to_printable_str(conf_path)))


def load_conf():

    conf_path = os.path.expanduser('~/.cos.conf')
    if not os.path.exists(conf_path):
        logger.warn("{conf} couldn't be found, please config tool!".format(conf=to_printable_str(conf_path)))
        raise IOError
    else:
        logger.debug('{conf} is found.'.format(conf=to_printable_str(conf_path)))

    with open(conf_path, 'r') as f:
        cp = SafeConfigParser()
        cp.readfp(fp=f)
        if cp.has_option('common', 'part_size'):
            part_size = cp.getint('common', 'part_size')
        else:
            part_size = 1

        if cp.has_option('common', 'max_thread'):
            max_thread = cp.getint('common', 'max_thread')
        else:
            max_thread = 5
        conf = CosConfig(
            appid=cp.get('common', 'appid'),
            access_id=cp.get('common', 'access_id'),
            access_key=cp.get('common', 'secret_key'),
            region=cp.get('common', 'region'),
            bucket=cp.get('common', 'bucket'),
            part_size=part_size,
            max_thread=max_thread
        )
        return conf


class Op(object):
    @staticmethod
    def upload(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.cos_path.startswith('/'):
            args.cos_path = args.cos_path[1:]
        Intface = client.op_int()

        if not isinstance(args.local_path, unicode):
            args.local_path = args.local_path.decode(fs_coding)
        if not isinstance(args.cos_path, unicode):
            args.cos_path = args.cos_path.decode(fs_coding)

        if not os.path.exists(args.local_path):
            logger.warn('local_path %s not exist!' % to_printable_str(args.local_path))
            return -1

        if not os.access(args.local_path, os.R_OK):
            logger.warn('local_path %s is not readable!' % to_printable_str(args.local_path))
            return -1
        if args.recursive:
            if os.path.isdir(args.local_path) is False:
                logger.warn(change_color("path not exist!", color_red))
                return -1
            rt = Intface.upload_folder(args.local_path, args.cos_path)
            logger.info(change_color("upload {file} finished".format(file=to_printable_str(args.local_path)), color_green))
            logger.info(change_color("totol of {folders} folders, {files} files".format(folders=Intface._folder_num, files=Intface._file_num), color_green))
            if rt:
                return 0
            else:
                return -1
        else:
            if os.path.isfile(args.local_path) is False:
                logger.warn(change_color("path not exist!", color_red))
                return -1
            if Intface.upload_file(args.local_path, args.cos_path) is True:
                logger.info(change_color("upload {file} success".format(file=to_printable_str(args.local_path)), color_green))
                return 0
            else:
                logger.warn(change_color("upload {file} fail".format(file=to_printable_str(args.local_path)), color_red))
                return -1
        return -1

    @staticmethod
    def download(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.cos_path.startswith('/'):
            args.cos_path = args.cos_path[1:]
        Intface = client.op_int()
        if not isinstance(args.local_path, unicode):
            args.local_path = args.local_path.decode(fs_coding)

        if not isinstance(args. cos_path, unicode):
            args.cos_path = args.cos_path.decode(fs_coding)

        if Intface.download_file(args.local_path, args.cos_path):
            logger.info(change_color("download success!", color_green))
            return 0
        else:
            logger.warn(change_color("download fail!", color_red))
            return -1

    @staticmethod
    def delete(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.cos_path.startswith('/'):
            args.cos_path = args.cos_path[1:]
        Intface = client.op_int()

        if not isinstance(args. cos_path, unicode):
            args.cos_path = args.cos_path.decode(fs_coding)

        if args.recursive:
            if Intface.delete_folder(args.cos_path):
                logger.info(change_color("delete success!", color_green))
                return 0
            else:
                logger.warn(change_color("delete fail!", color_red))
                return -1
        else:
            if Intface.delete_file(args.cos_path):
                logger.info(change_color("delete success!", color_green))
                return 0
            else:
                logger.warn(change_color("delete fail!", color_red))
                return -1

    @staticmethod
    def put_object_acl(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.cos_path.startswith('/'):
            args.cos_path = args.cos_path[1:]
        if not isinstance(args. cos_path, unicode):
            args.cos_path = args.cos_path.decode(fs_coding)
        Intface = client.op_int()
        rt = Intface.put_object_acl(args.grant_read, args.grant_write, args.grant_full_control, args.cos_path)
        if rt is True:
            logger.info(change_color("put success!", color_green))
            return 0
        else:
            logger.warn(change_color("put fail!", color_red))
            return -1

    @staticmethod
    def get_object_acl(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.cos_path.startswith('/'):
            args.cos_path = args.cos_path[1:]
        if not isinstance(args. cos_path, unicode):
            args.cos_path = args.cos_path.decode(fs_coding)
        Intface = client.op_int()

        rt = Intface.get_object_acl(args.cos_path)
        if rt is True:
            logger.info(change_color("get success!", color_green))
            return 0
        else:
            logger.warn(change_color("get fail!", color_red))
            return -1

    @staticmethod
    def create_bucket(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.op_int()
        if Intface.create_bucket():
            logger.info(change_color("create success!", color_green))
            return 0
        else:
            logger.warn(change_color("create fail!", color_red))
            return -1

    @staticmethod
    def delete_bucket(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.op_int()
        if args.force is True:
            if Intface.delete_folder("") is False:
                logger.warn(change_color("delete files in bucket fail", color_red))
                return -1
        if Intface.delete_bucket():
            logger.info(change_color("delete success!", color_green))
            return 0
        else:
            logger.warn(change_color("delete fail!", color_red))
            return -1

    @staticmethod
    def list_bucket(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.op_int()
        if Intface.get_bucket(args.max_keys):
            logger.info(change_color("list success!", color_green))
            return 0
        else:
            logger.warn(change_color("list fail!", color_red))
            return -1

    @staticmethod
    def put_bucket_acl(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.op_int()
        rt = Intface.put_bucket_acl(args.grant_read, args.grant_write, args.grant_full_control)
        if rt is True:
            logger.info(change_color("put success!", color_green))
            return 0
        else:
            logger.warn(change_color("put fail!", color_red))
            return -1

    @staticmethod
    def get_bucket_acl(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.op_int()
        rt = Intface.get_bucket_acl()
        if rt is True:
            logger.info(change_color("get success!", color_green))
            return 0
        else:
            logger.warn(change_color("get fail!", color_red))
            return -1


def _main():

    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose', help="verbose mode", action="store_true", default=False)

    sub_parser = parser.add_subparsers()
    parser_config = sub_parser.add_parser("config", help='''coscmd config [-h] -a ACCESS_ID -s SECRET_KEY -u APPID
                                                            -b BUCKET -r REGION [-m MAX_THREAD] [-p PART_SIZE]''')
    parser_config.add_argument('-a', '--access_id', help='specify your access id', type=str, required=True)
    parser_config.add_argument('-s', '--secret_key', help='specify your secret key', type=str, required=True)
    parser_config.add_argument('-u', '--appid', help='specify your appid', type=str, required=True)
    parser_config.add_argument('-b', '--bucket', help='specify your bucket', type=str, required=True)
    parser_config.add_argument('-r', '--region', help='specify your bucket', type=str, required=True)
    parser_config.add_argument('-m', '--max_thread', help='specify the number of threads (default 5)', type=int, default=5)
    parser_config.add_argument('-p', '--part_size', help='specify min part size in MB (default 1MB)', type=int, default=1)
    parser_config.set_defaults(func=config)

    parser_upload = sub_parser.add_parser("upload", help="coscmd  upload [-h] [-r] local_path cos_path")
    parser_upload.add_argument('local_path', help="local file path as /tmp/a.txt", type=str)
    parser_upload.add_argument("cos_path", help="cos_path as a/b.txt", type=str)
    parser_upload.add_argument('-r', '--recursive', help="upload folder", action="store_true", default=False)
    parser_upload.set_defaults(func=Op.upload)

    parser_download = sub_parser.add_parser("download", help="coscmd download [-h] cos_path local_path")
    parser_download.add_argument("cos_path", help="cos_path as a/b.txt", type=str)
    parser_download.add_argument('local_path', help="local file path as /tmp/a.txt", type=str)
    parser_download.set_defaults(func=Op.download)

    parser_delete = sub_parser.add_parser("delete", help="coscmd delete [-h] [-r] cos_path")
    parser_delete.add_argument("cos_path", help="cos_path as a/b.txt", type=str)
    parser_delete.add_argument('-r', '--recursive', help="delete folder", action="store_true", default=False)
    parser_delete.set_defaults(func=Op.delete)

#     parser_create_bucket = sub_parser.add_parser("createbucket", help='coscmd createbucket [-h]')
#     parser_create_bucket.set_defaults(func=Op.create_bucket)
#
#     parser_delete_bucket = sub_parser.add_parser("deletebucket", help='coscmd deletebucket [-h] [-f]')
#     parser_delete_bucket.add_argument('-f', '--force', help="force delete bucket", action="store_true", default=False)
#     parser_delete_bucket.set_defaults(func=Op.delete_bucket)
#
#     parser_list_bucket = sub_parser.add_parser("listbucket", help='coscmd listbucket [-h] [-m MAX_KEYS]')
#     parser_list_bucket.add_argument('-m', '--max_keys', help='specify max num you want to list', type=int, default=10)
#     parser_list_bucket.set_defaults(func=Op.list_bucket)
#
#     parser_put_object_acl = sub_parser.add_parser("putobjectacl", help='''coscmd putobjectacl [-h] [--grant-read GRANT_READ]
#                                                                        [--grant-write GRANT_WRITE]
#                                                                        [--grant-full-control GRANT_FULL_CONTROL]
#                                                                        cos_path''')
#     parser_put_object_acl.add_argument("cos_path", help="cos_path as a/b.txt", type=str)
#     parser_put_object_acl.add_argument('--grant-read', dest='grant_read', help='set grant-read', type=str, required=False)
#     parser_put_object_acl.add_argument('--grant-write', dest='grant_write', help='set grant-write', type=str, required=False)
#     parser_put_object_acl.add_argument('--grant-full-control', dest='grant_full_control', help='set grant-full-control', type=str, required=False)
#     parser_put_object_acl.set_defaults(func=Op.put_object_acl)
#
#     parser_get_object_acl = sub_parser.add_parser("getobjectacl", help='coscmd getobjectacl [-h] cos_path')
#     parser_get_object_acl.add_argument("cos_path", help="cos_path as a/b.txt", type=str)
#     parser_get_object_acl.set_defaults(func=Op.get_object_acl)
#
#     parser_put_bucket_acl = sub_parser.add_parser("putbucketacl", help='''coscmd putbucketacl [-h] [--grant-read GRANT_READ]
#                                                                        [--grant-write GRANT_WRITE]
#                                                                        [--grant-full-control GRANT_FULL_CONTROL]''')
#     parser_put_bucket_acl.add_argument('--grant-read', dest='grant_read', help='set grant-read', type=str, required=False)
#     parser_put_bucket_acl.add_argument('--grant-write', dest='grant_write', help='set grant-write', type=str, required=False)
#     parser_put_bucket_acl.add_argument('--grant-full-control', dest='grant_full_control', help='set grant-full-control', type=str, required=False)
#     parser_put_bucket_acl.set_defaults(func=Op.put_bucket_acl)
#
#     parser_get_bucket_acl = sub_parser.add_parser("getbucketacl", help='coscmd getbucketacl [-h]')
#     parser_get_bucket_acl.set_defaults(func=Op.get_bucket_acl)

    parser.add_argument('--version', action='version', version='%(prog)s 1.5.4.1')

    args = parser.parse_args()

    logger = logging.getLogger('')
    coloredlogs.DEFAULT_FIELD_STYLES = {'hostname': {'color': 'white'}, 'name': {'color': 'white'}, 'levelname': {'color': 'white', 'bold': True}, 'asctime': {'color': 'white'}}
    coloredlogs.DEFAULT_LEVEL_STYLES = {'info': {'color': 'white'}, 'warning': {'color': 'white', 'bold': True}}
    if args.verbose:
        coloredlogs.install(level='DEBUG', logger=logger, fmt='%(asctime)s - %(message)s')
    else:
        coloredlogs.install(level='INFO', logger=logger, fmt='%(asctime)s - %(message)s')

    return args.func(args)


if __name__ == '__main__':
    sys.exit(_main())