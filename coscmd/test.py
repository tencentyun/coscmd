# -*- coding=utf-8
import cos_client
import logging
import random
import sys
import os
reload(sys)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(message)s")
access_id = os.environ["ACCESS_ID"]
access_key = os.environ["ACCESS_KEY"]
test_num = 2
file_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))


def setUp():
    print "Test interface"


def tearDown():
    print "test over"


def gen_file(path, size):
    _file = open(path, 'w')
    _file.seek(1024*1024*size)
    _file.write('\x00')
    _file.close()


def Test():
    for i in range(test_num):
        bucket_id = str(random.randint(0, 1000)) + str(random.randint(0, 1000))
        conf = cos_client.CosConfig(
                    appid="1252448703",
                    bucket="test" + str(bucket_id),
                    region="cn-north",
                    access_id=access_id,
                    access_key=access_key,
                    part_size=1,
                    max_thread=5)
        client = cos_client.CosS3Client(conf)
        op_int = client.op_int()
        print "Test create bucket " + conf._bucket
        sys.stdout.flush()
        rt = op_int.create_bucket()
        assert rt
        print "Test get bucket " + conf._bucket
        sys.stdout.flush()
        rt = op_int.get_bucket()
        assert rt
        print "Test put bucket acl " + conf._bucket
        sys.stdout.flush()
        rt = op_int.put_bucket_acl("anyone,43,123", None, "anyone")
        assert rt
        print "Test get bucket acl " + conf._bucket
        sys.stdout.flush()
        rt = op_int.get_bucket_acl()
        assert rt
        print "Test delete bucket " + conf._bucket
        sys.stdout.flush()
        rt = op_int.delete_bucket()
        assert rt
        conf = cos_client.CosConfig(
            appid="1252448703",
            bucket="lewzylu06",
            region="cn-north",
            access_id=access_id,
            access_key=access_key,
            part_size=1,
            max_thread=5
        )
        client = cos_client.CosS3Client(conf)
        op_int = client.op_int()
        file_size = 5.1 * i + 0.1
        file_name = "tmp" + file_id + "_" + str(file_size) + "MB"
        print "Test upload " + file_name
        sys.stdout.flush()
        gen_file(file_name, file_size)
        rt = op_int.upload_file(file_name, file_name)
        assert rt
        print "Test put object acl " + file_name
        sys.stdout.flush()
        rt = op_int.put_object_acl("anyone,43,123", None, "anyone", file_name)
        assert rt
        print "Test get object acl " + file_name
        sys.stdout.flush()
        rt = op_int.get_object_acl(file_name)
        assert rt
        print "Test download " + file_name
        sys.stdout.flush()
        rt = op_int.download_file(file_name, file_name)
        assert rt
        os.remove(file_name)
        print "Test delete " + file_name
        sys.stdout.flush()
        rt = op_int.delete_file(file_name)
        assert rt


if __name__ == "__main__":
    setUp()
    Test()
