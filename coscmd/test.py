# -*- coding=utf-8
import logging
import random
import string
import sys
import os
import time
import filecmp
from _threading_local import local
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")
access_id = os.environ["COS_KEY"]
access_key = os.environ["COS_SECRET"]
bucket_name = "lewzylu" + str(random.randint(0, 1000)) + str(random.randint(0, 1000)) + "-1251668577"


def setUp():
    """Test interface"""
    os.system("python setup.py install")
    os.system("python coscmd/cos_cmd.py config -a %s -s %s -b %s -r ap-beijing-1" % (access_id, access_key, bucket_name))
    os.system("python coscmd/cos_cmd.py createbucket")
    time.sleep(5)


def tearDown():
    """delete testbucket"""
    os.system("python coscmd/cos_cmd.py delete -rf /")
    os.system("python coscmd/cos_cmd.py deletebucket")
    time.sleep(5)


def gen_file(filePath, fileSize):
    ds = 0
    with open(filePath, "w") as f:
        while ds < fileSize:
            f.write(str(round(random.uniform(-1000, 1000), 2)))
            f.write("\n")
            ds = os.path.getsize(filePath)


def check_file_same(local_path, cos_path):
    rt = os.system("python coscmd/cos_cmd.py download -f \"{cos_path}\" \"{local_path}_download\""
                   .format(cos_path=cos_path, local_path=local_path))
    if rt != 0:
        return rt
    rt = os.path.exists(local_path+"_download")
    if rt:
        rt = 0
    else:
        rt = -1
    try:
        os.remove("{local_path}".format(local_path=local_path))
        os.remove("{local_path}_download".format(local_path=local_path))
    except:
        pass
    return rt


def test_upload_file_01():
    """test upload file_tmp_tmp"""
    gen_file("tmp", 5.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    assert check_file_same("tmp", "tmp") == 0
    gen_file("tmp", 1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    assert check_file_same("tmp", "tmp") == 0


def test_upload_file_02():
    """test upload file_tmp_/"""
    local_path = "tmp"
    cos_path = "/"
    gen_file("tmp", 5.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp /")
    assert rt == 0
    assert check_file_same("tmp", "tmp") == 0

    gen_file("tmp", 1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp /")
    assert rt == 0
    assert check_file_same("tmp", "tmp") == 0


# def test_upload_file_03():
#     """test upload file_tmp_/home/"""
#     key = u"! #$%&\'()*+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
#     gen_file("tmp", 5.1)
#     rt = os.system(u"python coscmd/cos_cmd.py upload tmp \"{key}\"".format(key=key))
#     assert rt == 0

#     gen_file("tmp", 1)
#     rt = os.system(u"python coscmd/cos_cmd.py upload tmp \"{key}\"".format(key=key))
#     assert rt == 0


def test_upload_file_04():
    """test upload file_tmp_home/"""
    gen_file("tmp", 5.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp home/")
    assert rt == 0
    assert check_file_same("home/tmp", "tmp") == 0

    gen_file("tmp", 1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp home/")
    assert rt == 0
    assert check_file_same("home/tmp", "tmp") == 0


def test_download_file_01():
    """test download file_tmp_tmp"""
    gen_file("tmp", 7.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -f tmp tmp_download")
    assert rt == 0
    rt = os.path.exists("tmp_download")
    assert rt is True
    os.remove("tmp")
    os.remove("tmp_download")


def test_download_file_02():
    """test download file_tmp_testfolder/"""
    gen_file("tmp", 7.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -f tmp testfolder/")
    assert rt == 0
    rt = os.path.exists("testfolder/tmp")
    assert rt is True
    try:
        os.remove("tmp")
        os.remove("testfolder/tmp")
        os.removedirs("testfolder/")
    except:
        pass

# def test_download_file_03():
#     """test download file_tmp_/home/testfolder/"""
#     gen_file("tmp", 7.1)
#     rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
#     assert rt == 0
#     rt = os.system("python coscmd/cos_cmd.py download -f tmp /home/testfolder/")
#     assert rt == 0
#     rt = os.path.exists("/home/testfolder/tmp")
#     assert rt is True
#     os.remove("tmp")
#     os.remove("/home/testfolder/tmp")
#     os.removedirs("/home/testfolder/")


def test_bucketacl():
    """test bucketacl"""
    rt = os.system("python coscmd/cos_cmd.py putbucketacl --grant-read anyone --grant-write anyone --grant-full-control anyone")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py getbucketacl")
    assert rt == 0


def test_objectacl():
    """test objectacl"""
    gen_file("tmp", 1.1)
    rt = os.system("python coscmd/cos_cmd.py upload tmp tmp")
    assert rt == 0
    os.remove("tmp")
    rt = os.system("python coscmd/cos_cmd.py putobjectacl tmp --grant-read anyone --grant-write anyone --grant-full-control anyone")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py getobjectacl tmp")
    assert rt == 0


def test_folder():
    """test objectacl"""
    try:
        os.makedirs("testfolder/")
    except:
        pass
    gen_file("testfolder/tmp1", 1.1)
    gen_file("testfolder/tmp2", 1.1)
    gen_file("testfolder/tmp3", 1.1)
    rt = os.system("python coscmd/cos_cmd.py upload -r testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py upload -rs testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -rf testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py download -rsf testfolder testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py copy -r %s.cos.ap-beijing-1.myqcloud.com/testfolder testfolder2" % bucket_name)
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py list")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py delete -rf testfolder")
    assert rt == 0
    rt = os.system("python coscmd/cos_cmd.py delete -rf testfolder2")
    assert rt == 0
    os.remove("testfolder/tmp1")
    os.remove("testfolder/tmp2")
    os.remove("testfolder/tmp3")
    os.removedirs("testfolder/")


if __name__ == "__main__":
    setUp()
    tearDown()
