# -*- coding=utf-8
import logging
import random
import string
import sys
import os
import time
import filecmp
import hashlib
from _threading_local import local
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")
access_id = os.environ["COS_KEY"]
access_key = os.environ["COS_SECRET"]
region = os.environ['COS_REGION']
appid = os.environ['COS_APPID']
bucket_name = "lewzylu" + str(random.randint(0, 1000)) + str(random.randint(0, 1000)) + "-" + appid
special_file_name = "中文" + "→↓←→↖↗↙↘! \"#$%&'()*+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
file_name = "tmp"
test_file_num = 55
seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-"


def get_raw_md5(data):
    m2 = hashlib.md5(data)
    etag = '"' + str(m2.hexdigest()) + '"'
    return etag


def gen_file(path, size, random_num=2):
    sa = []
    for i in range(8):
        sa.append(random.choice(seed))
    salt = ''.join(sa)
    _file = open(path, 'w')
    for i in range(random_num):
        sk = random.randint(0, int(size * 1024 * 1024))
        _file.seek(sk)
        _file.write(salt)
    _file.close()


def gen_folder(num=1024):
    for i in range(num):
        gen_file("testfolder/testfile中文_" + str(i), 0.01, 2)
    for i in range(12):
        gen_file("testfolder/testfile_big中文_" + str(i), 30, 10)


def setUp():
    """create testbucket"""
    os.system("python3 setup.py install")
    os.system("python3 coscmd/cos_cmd.py config -a %s -s %s -b %s -r %s" % (access_id, access_key, bucket_name, region))
    print("创建bucket")
    os.system("python3 coscmd/cos_cmd.py createbucket >/dev/null 2>&1")
    time.sleep(5)


def tearDown():
    """delete testbucket"""
    print("删除bucket")
    os.system("python3 coscmd/cos_cmd.py delete -rf / >/dev/null 2>&1")
    os.system("python3 coscmd/cos_cmd.py deletebucket >/dev/null 2>&1")
    time.sleep(5)


def test_upload_object_1MB():
    """简单上传1MB小文件"""
    gen_file(file_name, 1)

    with open(file_name, 'rb') as f:
        etag = get_raw_md5(f.read())
    rt = os.system("python3 coscmd/cos_cmd.py upload {local_path} {cos_path} >/dev/null 2>&1".format(local_path=file_name, cos_path=file_name))
    assert rt == 0
    return etag


def test_download_object_1MB():
    """下载1MB小文件"""
    etag = test_upload_object_1MB()
    rt = os.system("python3 coscmd/cos_cmd.py download -f {cos_path} {local_path} >/dev/null 2>&1".format(local_path=file_name, cos_path=file_name))

    assert rt == 0
    with open(file_name, 'rb') as f:
        etag_download = get_raw_md5(f.read())
    assert etag_download == etag

    if os.path.exists(file_name):
        os.remove(file_name)


def test_upload_object_30MB():
    """简单上传30MB文件"""
    gen_file(file_name, 30)

    with open(file_name, 'rb') as f:
        etag = get_raw_md5(f.read())
    rt = os.system("python3 coscmd/cos_cmd.py upload {local_path} {cos_path} >/dev/null 2>&1".format(local_path=file_name, cos_path=file_name))
    assert rt == 0
    return etag


def test_download_object_30MB():
    """下载30MB文件"""
    etag = test_upload_object_30MB()
    rt = os.system("python3 coscmd/cos_cmd.py download -f {cos_path} {local_path} >/dev/null 2>&1".format(local_path=file_name, cos_path=file_name))

    assert rt == 0
    with open(file_name, 'rb') as f:
        etag_download = get_raw_md5(f.read())
    assert etag_download == etag

    if os.path.exists(file_name):
        os.remove(file_name)


def test_delete_object_1MB():
    """删除1MB小文件"""
    test_upload_object_1MB()
    rt = os.system("python3 coscmd/cos_cmd.py delete -f {cos_path} >/dev/null 2>&1".format(cos_path=file_name))
    assert rt == 0

    if os.path.exists(file_name):
        os.remove(file_name)


def test_bucketacl():
    """Bucket ACL测试"""
    rt = os.system("python3 coscmd/cos_cmd.py putbucketacl --grant-read anyone --grant-write anyone --grant-full-control anyone >/dev/null 2>&1")
    assert rt == 0
    rt = os.system("python3 coscmd/cos_cmd.py getbucketacl >/dev/null 2>&1")
    assert rt == 0


def test_probe():
    """探测测试"""
    rt = os.system("python3 coscmd/cos_cmd.py probe >/dev/null 2>&1")
    assert rt == 0


def test_upload_folder():
    """文件夹上传"""
    try:
        os.makedirs("testfolder/")
    except Exception:
        pass
    gen_folder(test_file_num)

    print("文件夹上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -r testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    print("文件夹同步上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -rs testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    print("文件夹同步上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -rs testfolder testfolder --include '*9,*7' >/dev/null 2>&1")
    assert rt == 0
    print("文件夹同步上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -rs testfolder testfolder --ignore '*1,*9' >/dev/null 2>&1")
    assert rt == 0
    os.system("rm -rf testfolder/")


def test_download_folder():
    """文件夹下载"""
    try:
        os.makedirs("testfolder/")
    except Exception:
        pass
    gen_folder(test_file_num)
    print("文件夹上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -r testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    time.sleep(5)
    print("文件夹下载")
    rt = os.system("python3 coscmd/cos_cmd.py download -rf testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    print("文件夹同步下载")
    rt = os.system("python3 coscmd/cos_cmd.py download -rsf testfolder testfolder >/dev/null 2>&1")
    print("文件夹同步下载include")
    rt = os.system("python3 coscmd/cos_cmd.py download -rsf testfolder testfolder --include '*9,*7' >/dev/null 2>&1")
    assert rt == 0
    print("文件夹同步下载ignore")
    rt = os.system("python3 coscmd/cos_cmd.py download -rsf testfolder testfolder --ignore '*1,*9' >/dev/null 2>&1")
    assert rt == 0
    os.system("rm -rf testfolder/")


def test_copy_folder():
    """文件夹复制"""
    try:
        os.makedirs("testfolder/")
    except Exception:
        pass
    gen_folder(test_file_num)
    print("文件夹上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -r testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    time.sleep(5)
    print("文件夹复制")
    rt = os.system("python3 coscmd/cos_cmd.py copy -r %s.cos.%s.myqcloud.com/testfolder testfolder2 >/dev/null 2>&1" % (bucket_name, region))
    assert rt == 0
    print("文件夹同步复制include")
    rt = os.system("python3 coscmd/cos_cmd.py copy -rs %s.cos.%s.myqcloud.com/testfolder testfolder2 --include '*9,*7' >/dev/null 2>&1" % (bucket_name, region))
    assert rt == 0
    print("文件夹同步复制ignore")
    rt = os.system("python3 coscmd/cos_cmd.py copy -rs %s.cos.%s.myqcloud.com/testfolder testfolder2 --ignore '*1,*9' >/dev/null 2>&1" % (bucket_name, region))
    assert rt == 0
    print("文件夹同步复制--delete")
    rt = os.system("python3 coscmd/cos_cmd.py copy -rs %s.cos.%s.myqcloud.com/testfolder testfolder2 --ignore '*1,*9' -f --delete >/dev/null 2>&1" % (bucket_name, region))
    assert rt == 0
    print("文件夹move")
    rt = os.system("python3 coscmd/cos_cmd.py move -r %s.cos.%s.myqcloud.com/testfolder testfolder2 >/dev/null 2>&1" % (bucket_name, region))
    assert rt == 0
    os.system("rm -rf testfolder/")


def test_list_folder():
    """文件夹打印"""
    try:
        os.makedirs("testfolder/")
    except Exception:
        pass
    gen_folder(test_file_num)
    print("文件夹上传")
    rt = os.system("python3 coscmd/cos_cmd.py upload -r testfolder testfolder >/dev/null 2>&1")
    assert rt == 0
    time.sleep(5)
    print("打印对象")
    rt = os.system("python3 coscmd/cos_cmd.py list -n 10 >/dev/null 2>&1")
    print("打印全部对象")
    rt = os.system("python3 coscmd/cos_cmd.py list -ar >/dev/null 2>&1")
    assert rt == 0
    os.system("rm -rf testfolder/")


if __name__ == "__main__":
    setUp()
    tearDown()
