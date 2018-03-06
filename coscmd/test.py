# -*- coding=utf-8
import cos_client
import logging
import random
import shutil
import sys
import os
import time
reload(sys)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")
access_id = os.environ["COS_KEY"]
access_key = os.environ["COS_SECRET"]
test_num = 2
file_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000)) + "中文"
conf = cos_client.CosConfig(
        appid="1252448703",
        bucket="lewzylu06",
        region="ap-beijing-1",
        secret_id=access_id,
        secret_key=access_key,
        part_size=1,
        max_thread=5
    )
client = cos_client.CosS3Client(conf)
op_int = client.op_int()


def setUp():
    print "Test interface"
    op_int.create_bucket()
    time.sleep(5)


def tearDown():
    """delete testbucket"""
    op_int.delete_folder(cos_path="", _force=True)
    op_int.delete_bucket()
    time.sleep(5)


def gen_file(path, size):
    _file = open(path, 'w')
    _file.seek(1024*1024*size)
    _file.write('\x00')
    _file.close()


def test_upload_small_file():
    """test upload small file"""
    file_name = "tmp" + file_id + "_Smallfile"
    gen_file("tmp", 1.1)
    rt = op_int.upload_file("tmp", file_name)
    assert rt
    os.remove("tmp")


def test_upload_big_file():
    """test upload small file"""
    file_name = "tmp" + file_id + "_Bigfile"
    gen_file("tmp", 5.1)
    rt = op_int.upload_file("tmp", file_name)
    assert rt
    os.remove("tmp")


def test_download_file():
    """test download file"""
    file_name = "tmp" + file_id + "_Bigfile"
    rt = op_int.download_file(file_name, "tmp", True)
    assert rt
    os.remove("tmp")


def test_delete_file():
    """test delete file"""
    file_name = "tmp" + file_id + "_Bigfile"
    rt = op_int.delete_file(file_name, _force=True)
    assert rt


def test_upload_folder():
    """test upload folder"""
    if os.path.isdir('testfolder') is False:
        os.mkdir('testfolder')
    gen_file('testfolder/1', 1.1)
    gen_file('testfolder/2', 2.1)
    gen_file('testfolder/3', 3.1)
    gen_file('testfolder/4', 4.1)
    gen_file('testfolder/5', 5.1)
    op_int.upload_folder('testfolder', 'testfolder')
    shutil.rmtree('testfolder/')


def test_download_folder():
    """test download folder"""
    op_int.download_folder('testfolder', 'testfolder')
    shutil.rmtree('testfolder/')


def test_delete_folder():
    """test delete folder"""
    op_int.delete_folder(cos_path='', _force=True)


def test_bucketacl():
    """test bucketacl"""
    op_int.put_bucket_acl("anyone", "anyone", "327874225")
    time.sleep(60)
    rt = op_int.get_bucket_acl()
    assert rt


def test_objectacl():
    """test objectacl"""
    file_name = "tmp" + file_id + "_Smallfile"
    gen_file("tmp", 1.1)
    op_int.upload_file("tmp", file_name)
    os.remove("tmp")
    op_int.put_object_acl("3210232098/327874225", "anyone", "", file_name)
    time.sleep(60)
    rt = op_int.get_object_acl(file_name)
    assert rt

if __name__ == "__main__":
    setUp()
