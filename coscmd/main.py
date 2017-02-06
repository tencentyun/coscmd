# -*- coding: utf-8 -*-

from __future__ import absolute_import
from coscmd.client import CosConfig, CosS3Client
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from os import path
import random
import sys
import time
import logging

logger = logging.getLogger(__name__)

def config(args):
    logger.debug("config: " + str(args))

    conf_path = path.expanduser('~/.cos.conf')

    with open(conf_path, 'w+') as f:
        cp = SafeConfigParser()
        cp.add_section("common")
        cp.set('common', 'access_id', args.access_id)
        cp.set('common', 'secret_key', args.secret_key)
        cp.set('common', 'appid', args.appid)
        cp.set('common', 'bucket', args.bucket)
        cp.set('common', 'region', args.region)
        cp.set('common', 'part_size', str(args.part_size))
        cp.write(f)
        logger.info("Created configuration file in {path}".format(path=conf_path))


def load_conf():

    conf_path = path.expanduser('~/.cos.conf')
    if not path.exists(conf_path):
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
        conf = CosConfig(
            appid=cp.get('common', 'appid'),
            access_id=cp.get('common', 'access_id'),
            access_key=cp.get('common', 'secret_key'),
            region=cp.get('common', 'region'),
            bucket=cp.get('common', 'bucket'),
            part_size = part_size
        )
        return conf


def upload(args):
    conf = load_conf()
    client = CosS3Client(conf)
    while args.object_name.startswith('/'):
      args.object_name = args.object_name[1:]

    mp = client.multipart_upload_from_filename(args.local_file, args.object_name)

    retry = 5
    
    for i in range(retry):
      wait_time = random.randint(0, 20)
      logger.debug("begin to init upload part after {second} second".format(second=wait_time))
      time.sleep(wait_time)
      rt = mp.init_mp()
      if rt:
        break
    else:
      return -1
    logger.warn("Init multipart upload ok")

    for i in range(retry):
      rt = mp.upload_parts()
      if rt:
        break
    else:
      return -1
    logger.warn("multipart upload ok")
  
    for i in range(retry):
      wait_time = random.randint(0, 5)
      time.sleep(wait_time)
      logger.debug("begin to complete upload part after {second} second".format(second=wait_time))
      rt = mp.complete_mp()
      if rt:
        logger.warn("complete multipart upload ok")
        return 0
    logger.warn("complete multipart upload failed")
    return -1
    
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
    parser_a.add_argument('-p', '--part_size', help='specify min part size in MB (default 1MB)', type=int, default=1) 
    parser_a.set_defaults(func=config)

    parser_b = sub_parser.add_parser("upload")
    parser_b.add_argument('local_file', help="local file path as /tmp/a.txt", type=str)
    parser_b.add_argument("object_name", help="object name as a/b.txt", type=str)
    parser_b.add_argument("-t", "--type", help="storage class type: standard/nearline/coldline", type=str, choices=["standard", "nearline", "coldline"], default="standard")
    parser_b.set_defaults(func=upload)

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format="%(asctime)s - %(message)s")
    else :
        logging.basicConfig(level=logging.WARN, stream=sys.stdout, format="%(asctime)s - %(message)s")

    return args.func(args)

if __name__ == '__main__':
    _main()
