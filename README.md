## ����˵��
ʹ�� COSCMD ���ߣ��û���ͨ���򵥵�������ָ��ʵ�ֶԶ���Object���������ϴ������ء�ɾ���Ȳ�����
## ʹ������
1. ������ COS V4��V5 �汾��

## ʹ�û���
### ϵͳ����
Windows �� Linux ϵͳ
### �������
Python 2.7
��װ�����°汾��pip
#### ��װ������
������װ��������ϸ������ο� [Python ��װ������](https://cloud.tencent.com/document/product/436/10866)��
## �����밲װ
- **�ֶ���װ**
�������ӣ�[GitHub ����](https://github.com/tencentyun/coscmd.git)
�ڸ���Ŀ��Ŀ¼��ʹ���������װ
```
python setup.py install
```
- **pip ��װ**
ִ��`pip`������а�װ��
```
pip install coscmd
```
��װ�ɹ�֮���û�����ͨ��`-v`����`--version`����鿴��ǰ�İ汾��Ϣ��
- **pip ����**
��ִ��`pip`������и��£�
```
pip install coscmd -U
```
**ע�ⲻ������linux����windows�����£�������ͨ�����ϵķ�����װ�����**
## ʹ�÷���
### �鿴 help
�û���ͨ��`-h`��`--help`�������鿴���ߵ� help ��Ϣ��
```
coscmd -h  //�鿴����汾��Ϣ
```
help ��Ϣ������ʾ��
```
usage: coscmd [-h] [-d] [-b BUCKET] [-v]
              {config,upload,download,delete,list,info,mget,restore,signurl,createbucket,deletebucket,putobjectacl,getobjectacl,putbucketacl,getbucketacl}
              ...

an easy-to-use but powerful command-line tool. try 'coscmd -h' to get more
informations. try 'coscmd sub-command -h' to learn all command usage, likes
'coscmd upload -h'

positional arguments:
  {config,upload,download,delete,list,info,mget,restore,signurl,createbucket,deletebucket,putobjectacl,getobjectacl,putbucketacl,getbucketacl}
    config              config your information at first.
    upload              upload file or directory to COS.
    download            download file from COS to local.
    delete              delete file or files on COS
    list                list files on COS
    info                get the information of file on COS
    mget                download big file from COS to local(Recommand)
    restore             restore
    signurl             get download url
    createbucket        create bucket
    deletebucket        delete bucket
    putobjectacl        set object acl
    getobjectacl        get object acl
    putbucketacl        set bucket acl
    getbucketacl        get bucket acl

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           debug mode
  -b BUCKET, --bucket BUCKET
                        set bucket
  -v, --version         show program's version number and exit
```
����֮�⣬�û���������ÿ������󣨲��Ӳ���������`-h`�鿴������ľ����÷������磺
```
coscmd upload -h  //�鿴 upload ����ʹ�÷���
```
### ���ò���
COSCMD ������ʹ��ǰ��Ҫ���в������á��û�����ͨ���������������ã�
```
coscmd config -a <access_id> -s <secret_key> -b <bucket> -r <region> [-m <max_thread>] [-p <parts_size>]      
```
����ʾ����ʹ��"<>"���ֶ�Ϊ��ѡ������ʹ��"[]"���ֶ�Ϊ��ѡ���������У�

| ����         | ����                                       | ��Чֵ  |
| :---------: | :----------------------------------------: | :----: |
| secret_id  | ��ѡ������APPID ��Ӧ����Կ ID���ɴӿ���̨��ȡ���ο� [��������](https://cloud.tencent.com/doc/product/436/6225)�� | �ַ���  |
| secret_key | ��ѡ������APPID ��Ӧ����Կ Key���ɴӿ���̨��ȡ���ο� [��������](https://cloud.tencent.com/doc/product/436/6225)�� | �ַ���  |
| bucket     | ��ѡ������ָ���Ĵ洢Ͱ���ƣ�bucket����������Ϊ{name}-{appid} ���ο� [�����洢Ͱ](https://cloud.tencent.com/doc/product/436/6232)�� | �ַ���  |
| region     | ��ѡ�������洢Ͱ���ڵ��򡣲ο� [���õ���](https://cloud.tencent.com/doc/product/436/6224)�� | �ַ���  |
| max_thread | ��ѡ���������߳��ϴ�ʱ������߳�����Ĭ��Ϊ 5������Чֵ��1~10         | ����   |
| parts_size | ��ѡ�������ֿ��ϴ��ĵ����С����λΪ M��Ĭ��Ϊ 1M������Чֵ��1~10     | ����   |

Ҳ����ֱ�ӱ༭`~/.cos.conf`�ļ� ������windows�����£����ļ���λ��`�ҵ��ĵ�`�µ�һ�������ļ�����
�������֮���`.cos.conf`�ļ�����ʾ��������ʾ��
```
 [common]
secret_id = AChT4ThiXAbpBDEFGhT4ThiXAbpHIJK
secret_key = WE54wreefvds3462refgwewerewr
bucket = ABC-1234567890
region = cn-south
max_thread = 5
part_size = 1
```
### ָ��bucket������
-  ͨ��`-b <bucket> ����ָ��bucket`
- bucket����������Ϊ`{name}-{appid}` ���˴���д�Ĵ洢Ͱ���Ʊ���Ϊ�˸�ʽ
```
coscmd -b <bucket> method ...  //�����ʽ
coscmd -b AAA-12345567 upload a.txt b.txt  //����ʾ��-�ϴ��ļ�
coscmd -b AAA-12344567 createbucket  //����ʾ��-����bucket
```

### ����bucket
-  �������`-b <bucket> ָ��bucket`ʹ��
```
coscmd -b <bucket> createbucket //�����ʽ
coscmd createbucket  //����ʾ��
coscmd -b AAA-12344567 createbucket  //����ʾ��
```

### ɾ��bucket
-  �������`-b <bucket> ָ��bucket`ʹ��
```
coscmd -b <bucket> deletebucket //�����ʽ
coscmd createbucket  //����ʾ��
coscmd -b AAA-12344567 deletebucket  //����ʾ��
```
### �ϴ��ļ����ļ���
- �ϴ��ļ��������£�
```
coscmd upload <localpath> <cospath>  //�����ʽ
coscmd upload /home/aaa/123.txt bbb/123.txt  //����ʾ��
coscmd upload /home/aaa/123.txt bbb/  //����ʾ��
```
- �ϴ��ļ����������£�
```
coscmd upload -r <localpath> <cospath>  //�����ʽ
coscmd upload -r /home/aaa/ bbb/aaa  //����ʾ��
coscmd upload -r /home/aaa/ bbb/  //����ʾ��
coscmd upload -r /home/aaa/ /  //�ϴ���bucket��Ŀ¼
```

�뽫 "<>" �еĲ����滻Ϊ����Ҫ�ϴ��ı����ļ�·����localpath�����Լ� COS �ϴ洢��·����cospath����
**ע�⣺** 
1. �ϴ��ļ�ʱ��Ҫ��cos�ϵ�·�������ļ�(��)�����ֲ�ȫ(�ο�����)��
2. COSCMD ֧�ִ��ļ��ϵ��ϴ����ܡ�����Ƭ�ϴ����ļ�ʧ��ʱ�������ϴ����ļ�ֻ���ϴ�ʧ�ܵķֿ飬�������ͷ��ʼ���뱣֤�����ϴ����ļ���Ŀ¼�Լ����ݺ��ϴ���Ŀ¼����һ�£���
3. COSCMD �ֿ��ϴ�ʱ���ÿһ�����md5У��

### �����ļ����ļ���
�����ļ��������£�
```
coscmd download <cospath> <localpath>  //�����ʽ
coscmd download bbb/123.txt /home/aaa/111.txt  //����ʾ��
coscmd download bbb/123.txt /home/aaa/  //����ʾ��
```
- ���������ļ����������£�
```
coscmd download-r <cospath> <localpath> //�����ʽ
coscmd download -r /home/aaa/ bbb/aaa  //����ʾ��
coscmd download -r /home/aaa/ bbb/  //����ʾ��
coscmd download -r / bbb/aaa  //���ص�ǰbucket��Ŀ¼�����е��ļ�
```
�뽫 "<>" �еĲ����滻Ϊ����Ҫ���ص� COS ���ļ���·����cospath�����Լ����ش洢·����localpath����
**ע�⣺** 
1. �����ش���ͬ���ļ����������ʧ�ܡ�ʹ�� `-f` �������Ǳ����ļ�
2. �����������е� `download` �滻Ϊ `mget`�� �����ʹ�÷ֿ����أ��ڴ����㹻���������ٶȻ�����2-3����

### ɾ���ļ����ļ���
- ɾ���ļ��������£�
```
coscmd delete <cospath>  //�����ʽ
coscmd delete bbb/123.txt  //����ʾ��
```
- ����ɾ���ļ����������£�
```
coscmd delete -r <cospath>  //�����ʽ
coscmd delete -r bbb/  //����ʾ��
coscmd delete -r /  //����ʾ��
```

�뽫"<>"�еĲ����滻Ϊ����Ҫɾ���� COS ���ļ���·����cospath�������߻���ʾ�û��Ƿ�ȷ�Ͻ���ɾ��������
**ע�⣺** 
1. ����ɾ����Ҫ����ȷ����ʹ�� `-f` ��������ȷ�� 

### �����ļ�
- �����ļ��������£�
```
coscmd copy <sourcepath> <cospath>  //�����ʽ
coscmd copybucket-appid.cos.ap-guangzhou.myqcloud.com/a.txt aaa/123.txt  //����ʾ��
```

�뽫"<>"�еĲ����滻Ϊ����Ҫ���Ƶ� COS ���ļ���·����sourcepath����������Ҫ���Ƶ� COS ���ļ���·����cospath����

**ע�⣺** 
1. sourcepath����ʽ���£�<bucketname>-<appid>.cos.<region>.myqcloud.com/<cospath>

### ��ӡ�ļ��б�
- ��ӡ�������£�
```
coscmd list <cospath>  //�����ʽ
coscmd list -a //����ʾ��
coscmd list bbb/123.txt  -r -n 10 //����ʾ��
```
�뽫"<>"�еĲ����滻Ϊ����Ҫ��ӡ�ļ��б�� COS ���ļ���·����cospath����
* ʹ��`-a`��ӡȫ���ļ�
* ʹ�� `-r` �ݹ��ӡ�����һ���ĩβ�����г��ļ��������ʹ�С֮��
* ʹ�� `-n num` ���ô�ӡ���������ֵ

**ע�⣺** 
1. <cospath>Ϊ��Ĭ�ϴ�ӡ��ǰBucket��Ŀ¼

### ��ʾ�ļ���Ϣ
- �������£�
```
coscmd info <cospath>  //�����ʽ
coscmd info bbb/123.txt //����ʾ��
```
�뽫"<>"�еĲ����滻Ϊ����Ҫ��ʾ�� COS ���ļ���·����cospath����

### ��ȡ��ǩ��������url
- �������£�
```
coscmd sigurl<cospath>  //�����ʽ
coscmd signurl bbb/123.txt //����ʾ��
coscmd signurl bbb/123.txt -t 100//����ʾ��
```
�뽫"<>"�еĲ����滻Ϊ����Ҫ��ȡ����url�� COS ���ļ���·����cospath����
* ʹ�� `-t time` ���ô�ӡǩ������Чʱ��(��λΪ��)

### ���÷��ʿ���(ACL)
- �������£�

ʹ��������������bucket�ķ��ʿ��ƣ�

```
coscmd putbucketacl [--grant-read GRANT_READ]  [--grant-write GRANT_WRITE] [--grant-full-control GRANT_FULL_CONTROL] //�����ʽ
coscmd putbucketacl --grant-read 12345678,12345678/11111 --grant-write anyone --grant-full-control 12345678/22222 //����ʾ��
```
ʹ��������������object�ķ��ʿ��ƣ�
```
coscmd putbucketacl [--grant-read GRANT_READ] [--grant-write GRANT_WRITE] [--grant-full-control GRANT_FULL_CONTROL] <cospath> //�����ʽ
coscmd putbucketacl --grant-read 12345678,12345678/11111 --grant-write anyone --grant-full-control 12345678/22222 aaa/aaa.txt //����ʾ��
```
* ACL����ָ��

 --grant-read�������Ȩ�ޡ�
 
--grant-write����д��Ȩ�ޡ�

--grant-full-control�����д��Ȩ�ޡ�

GRANT_READ / GRANT_WRITE / GRANT_FILL_CONTORL������Ȩ���ʺš�

����Ȩ���ʺţ�ʹ��rootid����ʽ��

����Ȩ���˻���ʹ��rootid/subid����ʽ��

����Ҫ�������˸�Ȩ��ʹ��anyone����ʽ��

ͬʱ��Ȩ�Ķ���ʺ��ö���(,)������

�뽫�����滻Ϊ������Ҫɾ����cos���ļ���·��(cospath)��

�����÷����ʾ����

### ��ȡ���ʿ���(ACL)
ʹ��������������bucket�ķ��ʿ��ƣ�
```
coscmd getbucketacl //�����ʽ
coscmd getbucketacl //����ʾ��
```
ʹ��������������object�ķ��ʿ��ƣ�
```
coscmd putbucketacl <cospath> //�����ʽ
coscmd getobjectacl aaa/aaa.txt //����ʾ��
```
### �ָ��鵵�ļ�
- �������£�
```
coscmd restore <cospath>  //�����ʽ
coscmd restore a.txt -d 3 -t  Expedited//����ʾ��
coscmd restore a.txt -d 3 -t  Bulk///����ʾ��
```
�뽫"<>"�еĲ����滻Ϊ����Ҫ��ӡ�ļ��б�� COS ���ļ���·����cospath����
* ʹ�� `-d day` ������ʱ�����Ĺ���ʱ�䣻Ĭ��ֵ��7
* ʹ�� `-t tier` ���帴ԭ�������ͣ�ö��ֵ�� Expedited ��Standard ��Bulk��Ĭ��ֵ��Standard

### debug ģʽִ������
�ڸ�����ǰ����`-d`����`-debug`��������ִ�еĹ����У�����ʾ��ϸ�Ĳ�����Ϣ ��ʾ�����£�
```
//��ʾupload����ϸ������Ϣ
coscmd -d upload <localpath> <cospath>  //�����ʽ
coscmd -d upload /home/aaa/123.txt bbb/123.txt  //����ʾ��
```
