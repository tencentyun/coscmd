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


def setUp():
    print "Test interface"
    os.system("python coscmd/cos_cmd.py config -a %s -s %s -b lewzylu06-1251668577 -r ap-beijing-1" % (access_id, access_key))
    os.system("python coscmd/cos_cmd.py createbucket")
    time.sleep(5)


def tearDown():
    """delete testbucket"""
    os.system("python coscmd/cos_cmd.py delete -rf /")
    time.sleep(5)


def gen_file(filePath, fileSize):
    ds = 0
    with open(filePath, "w") as f:
        while ds < fileSize:
            f.write(str(round(random.uniform(-1000, 1000), 2)))
            f.write("\n")
            ds = os.path.getsize(filePath)
    # print(os.path.getsize(filePath))


def test_upload_small_file():
    """test upload small file"""
    gen_file("tmp", 1.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    os.remove("tmp")


def test_upload_big_file():
    """test upload small file"""
    gen_file("tmp", 5.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    os.remove("tmp")


def test_download_file():
    """test download file"""
    gen_file("tmp", 7.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -f tmp tmp_download")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py delete -f tmp")
    assert rt == 0
    os.remove("tmp")


def test_bucketacl():
    """test bucketacl"""
    rt = os.system("python coscmd/cos_cmd.py putbucketacl --grant-read anyone --grant-write anyone --grant-full-control 327874225")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py getbucketacl")
    assert rt == 0


def test_objectacl():
    """test objectacl"""
    gen_file("tmp", 1.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    os.remove("tmp")
    rt = os.system("python coscmd/cos_cmd.py putobjectacl tmp --grant-read anyone --grant-write anyone --grant-full-control 327874225")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py getobjectacl tmp")
    assert rt == 0


def test_folder():
    """test objectacl"""
    file_name = "tmp" + file_id + "_Smallfile"
    try:
        os.makedirs("testfolder/")
    except:
        pass
    gen_file("testfolder/tmp1", 1.1)
    gen_file("testfolder/tmp2", 1.1)
    gen_file("testfolder/tmp3", 1.1)
    rt = os.system("python coscmd/cos_cmd.py upload -r testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -rf testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py delete -rf testfolder")
    assert rt == 0
    os.remove("testfolder/tmp1")
    os.remove("testfolder/tmp2")
    os.remove("testfolder/tmp3")
    os.removedirs("testfolder/")

if __name__ == "__main__":
    setUp()
    tearDown()
