# -*- coding: utf-8 -*-
from cos_client import CosConfig, CosS3Client
from ConfigParser import SafeConfigParser
from argparse import ArgumentParser
import random
import sys
import time
import logging
import os
logger = logging.getLogger(__name__)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


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
        logger.info("Created configuration file in {path}".format(path=conf_path))


def load_conf():

    conf_path = os.path.expanduser('~/.cos.conf')
    if not os.path.exists(conf_path):
        logger.warn("{conf} couldn't be found, please config tool!".format(conf=conf_path))
        raise IOError
    else:
        logger.info('{conf} is found.'.format(conf=conf_path))

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
            part_size = part_size,
            max_thread = max_thread
        )
        return conf
class FileOp(object):
    #文件上传
    @staticmethod
    def upload(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.object_name.startswith('/'):
          args.object_name = args.object_name[1:]
        Intface = client.obj_int()
        
        if not isinstance(args.local_file, unicode):
            args.local_file = args.local_file.decode('gbk')
        if not isinstance(args.object_name, unicode):
            args.object_name = args.object_name.decode('gbk')
            
        if not os.path.exists(args.local_file):
            self._err_tips = 'local_folder %s not exist!' % local_path
            return False
        
        if not os.access(args.local_file, os.R_OK):
            self._err_tips = 'local_folder %s is not readable!' % local_path
            return False
        if os.path.isdir(args.local_file):
            Intface.upload_folder(args.local_file, args.object_name)
            logger.info("upload {file} finished".format(file=args.local_file))
            logger.info("totol of {folders} folders, {files} files".format(folders=Intface._folder_num, files=Intface._file_num))
        elif os.path.isfile(args.local_file):
            if Intface.upload_file(args.local_file, args.object_name) == True:
                logger.info("upload {file} success".format(file=args.local_file))
            else:
                logger.info("upload {file} fail".format(file=args.local_file))
        else:
            logger.info("file or folder not exsist!")
    
    #文件下载
    @staticmethod
    def download(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.object_name.startswith('/'):
          args.object_name = args.object_name[1:]
        Intface = client.obj_int()
        
        if not isinstance(args.local_file, unicode): 
            args.local_file = args.local_file.decode('gbk')
        if not isinstance(args. object_name, unicode):
            args.object_name = args.object_name.decode('gbk')
        if Intface.download_file(args.local_file, args.object_name):
            logger.info("download success!")
        else:
            logger.info("download fail!")
            
    #文件删除
    @staticmethod
    def delete(args):
        conf = load_conf()
        client = CosS3Client(conf)
        while args.object_name.startswith('/'):
          args.object_name = args.object_name[1:]
        Intface = client.obj_int()
        
        if not isinstance(args. object_name, unicode):
            args.object_name = args.object_name.decode('gbk')
        if Intface.delete_file(args.object_name):
            logger.info("delete success!")
        else:
            logger.info("delete fail!")
    
class BucketOp(object):
    #创建bucket
    @staticmethod
    def create(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.buc_int()
        if Intface.create_bucket():
            logger.info("create success!")
        else:
            logger.info("create fail!")
        
    #删除bucket     
    @staticmethod
    def delete(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.buc_int()
        if Intface.delete_bucket():
            logger.info("delete success!")
        else:
            logger.info("delete fail!")#删除bucket  
  
    @staticmethod
    def list(args):
        conf = load_conf()
        client = CosS3Client(conf)
        Intface = client.buc_int()
        if Intface.get_bucket():
            logger.info("list success!")
            logger.info("save as tmp.xml in the current directory！")
        else:
            logger.info("list fail!")
        
def _main():
    
    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose', help="verbose mode", action="store_true", default=False)
    #初始化设置
    sub_parser = parser.add_subparsers(help="config")
    parser_a = sub_parser.add_parser("config")
    parser_a.add_argument('-a', '--access_id', help='specify your access id', type=str, required=True)
    parser_a.add_argument('-s', '--secret_key', help='specify your secret key', type=str, required=True)
    parser_a.add_argument('-u', '--appid', help='specify your appid', type=str, required=True)
    parser_a.add_argument('-b', '--bucket', help='specify your bucket', type=str, required=True)
    parser_a.add_argument('-r', '--region', help='specify your bucket', type=str, required=True)
    parser_a.add_argument('-m', '--max_thread', help='specify the number of threads (default 5)', type=int, default=5) 
    parser_a.add_argument('-p', '--part_size', help='specify min part size in MB (default 1MB)', type=int, default=1) 
    parser_a.set_defaults(func=config)
    
    #上传文件
    parser_b = sub_parser.add_parser("upload")
    parser_b.add_argument('local_file', help="local file path as /tmp/a.txt", type=str)
    parser_b.add_argument("object_name", help="object name as a/b.txt", type=str)
    parser_b.add_argument("-t", "--type", help="storage class type: standard/nearline/coldline", type=str, choices=["standard", "nearline", "coldline"], default="standard")
    parser_b.set_defaults(func=FileOp.upload)
    
    #下载文件
    parser_c = sub_parser.add_parser("download")
    parser_c.add_argument('local_file', help="local file path as /tmp/a.txt", type=str)
    parser_c.add_argument("object_name", help="object name as a/b.txt", type=str)
    parser_c.set_defaults(func=FileOp.download)

    #删除文件
    parser_d = sub_parser.add_parser("delete")
    parser_d.add_argument("object_name", help="object name as a/b.txt", type=str)
    parser_d.set_defaults(func=FileOp.delete)
    
    #
    parser_e = sub_parser.add_parser("create")
    parser_e.set_defaults(func=BucketOp.create)
    
    parser_f = sub_parser.add_parser("delete")
    parser_f.set_defaults(func=BucketOp.delete)
    
    parser_f = sub_parser.add_parser("list")
    parser_f.set_defaults(func=BucketOp.list)

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format="%(asctime)s - %(message)s")
    else :
        logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")

    return args.func(args)

if __name__ == '__main__':
    _main()
    
