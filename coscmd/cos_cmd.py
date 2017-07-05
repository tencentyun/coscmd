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

#文件上传
def upload(args):
    conf = load_conf()
    client = CosS3Client(conf)
    while args.cos_path.startswith('/'):
      args.cos_path = args.cos_path[1:]
    Intface = client.obj_int()
    
    if isinstance(args.local_path, unicode):
        pass
    else:
        args.local_path = args.local_path.decode('gbk')
    if isinstance(args.cos_path, unicode):
        pass
    else:
        args.cos_path = args.cos_path.decode('gbk')
        
    if not os.path.exists(args.local_path):
        logger.info('local_path %s not exist!' % args.local_path)
        return -1
    
    if not os.access(args.local_path, os.R_OK):
        logger.info('local_path %s is not readable!' % args.local_path)
        return -1
    if os.path.isdir(args.local_path):
        rt = Intface.upload_folder(args.local_path, args.cos_path)
        logger.info("upload {file} finished".format(file=args.local_path))
        logger.info("totol of {folders} folders, {files} files".format(folders=Intface._folder_num, files=Intface._file_num))
        if rt:
            return 0
        else:
            return -1;
    elif os.path.isfile(args.local_path):
        if Intface.upload_file(args.local_path, args.cos_path):
            logger.info("upload {file} success".format(file=args.local_path))
            return 0;
        else:
            logger.info("upload {file} fail".format(file=args.local_path))
            return -1;
    else:
        logger.info("file or folder not exsist!")
        return -1
    return -1;

def _main():
    
    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose', help="verbose mode", action="store_true", default=False)
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

    parser_b = sub_parser.add_parser("upload")
    parser_b.add_argument('local_path', help="local file path as /tmp/a.txt", type=str)
    parser_b.add_argument("cos_path", help="object path as a/b.txt", type=str)
    parser_b.add_argument("-t", "--type", help="storage class type: standard/nearline/coldline", type=str, choices=["standard", "nearline", "coldline"], default="standard")
    parser_b.set_defaults(func=upload)

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format="%(asctime)s - %(message)s")
    else :
        logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")

    return args.func(args)

if __name__ == '__main__':
    _main()
    
