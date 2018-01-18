# -*- coding=utf-8
import cos_client
import logging
import random
import shutil
import sys
import os
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


def tearDown():
    """delete testbucket"""
    rt = op_int.delete_folder(cos_path="", _force=True)


def gen_file(path, size):
    _file = open(path, 'w')
    _file.seek(1024*1024*size)
    _file.write('\x00')
    _file.close()


def test_upload_small_file():
    file_name = "tmp" + file_id + "_Smallfile"
    print "Test upload " + file_name
    sys.stdout.flush()
    gen_file("tmp", 1.1)
    rt = op_int.upload_file("tmp", file_name)
    assert rt
    os.remove("tmp")


def test_upload_big_file():
    file_name = "tmp" + file_id + "_Bigfile"
    print "Test upload " + file_name
    sys.stdout.flush()
    gen_file("tmp", 5.1)
    rt = op_int.upload_file("tmp", file_name)
    assert rt
    os.remove("tmp")


def test_download_file():
    file_name = "tmp" + file_id + "_Bigfile"
    print "Test download " + file_name
    sys.stdout.flush()
    rt = op_int.download_file(file_name, "tmp", True)
    assert rt
    os.remove("tmp")


def test_delete_file():
    file_name = "tmp" + file_id + "_Bigfile"
    print "Test delete " + file_name
    sys.stdout.flush()
    rt = op_int.delete_file(file_name, _force=True)
    assert rt


def test_upload_folder():
    if os.path.isdir('testfolder') is False:
        os.mkdir('testfolder')
    gen_file('testfolder/1', 1.1)
    gen_file('testfolder/2', 2.1)
    gen_file('testfolder/3', 3.1)
    gen_file('testfolder/4', 4.1)
    gen_file('testfolder/5', 5.1)
    print "Test upload folder"
    sys.stdout.flush()
    rt = op_int.upload_folder('testfolder', 'testfolder')
    shutil.rmtree('testfolder/')


def test_download_folder():
    print "Test download folder"
    sys.stdout.flush()
    rt = op_int.download_folder('testfolder', 'testfolder')
    shutil.rmtree('testfolder/')


def test_delete_folder():
    print "Test delete folder"
    sys.stdout.flush()
    rt = op_int.delete_folder(cos_path='', _force=True)


if __name__ == "__main__":
    setUp()
