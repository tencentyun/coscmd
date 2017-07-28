COSCMD使用文档
========


依赖
--------

操作系统为linux, python版本为2.7，系统安装有pip（可通过yum/apt来安装，包名为python-pip）。


安装
--------

在该项目根目录下执行如下命令安装:

.. code::
 
 pip install coscmd


使用方法
--------

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过如下命令来配置。


配置参数
^^^^^^^^

.. code::

 coscmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r region -m max_thread -p parts_size


请将参数替换为您的真实id/key/appid/bucket和园区代号,园区(region)为cn-south或者cn-north。

max_thread为多线程上传时的最大线程数(默认为5)

parts_size为分块上传的单块大小(单位为M)(默认为1M)


上传文件(夹)
^^^^^^^^

使用如下命令上传文件：

.. code::

 coscmd upload localpath cospath 

请将参数替换为您所需要的本地文件路径(localpath)，以及cos上存储的路径(cospath)。

支持大文件断点上传为功能。

当分片上传大文件失败时，重新上传该文件只会上传失败的分块，而不会从头开始(请保证重新上传的文件绝对目录以及内容和上传的目录不要改变)

使用-r删除文件夹。


下载文件
^^^^^^^^

使用如下命令上传文件：

.. code::

 coscmd download cospath localpath

请将参数替换为您所需要下载的cos上文件的路径(cospath)，以及需要的本地存储路径(localpath)。


删除文件
^^^^^^^^

使用如下命令删除文件：

.. code::

 coscmd delete cospath 

请将参数替换为您所需要删除的cos上文件的路径(cospath)。使用-r删除文件夹。


新建bucket
^^^^^^^^

使用如下命令新建bucket：

.. code::

 coscmd createbucket

输入以下命令会根据在conf设置的信息新建一个对应的bucket


删除bucket
^^^^^^^^

使用如下命令创建bucket：

.. code::

 coscmd deletebucket

输入以下命令会根据在conf设置的信息删除对应的bucket


遍历bucket
^^^^^^^^

使用如下命令遍历bucket中的文件：

.. code::

 coscmd listbucket

输入以下命令会根据在conf设置的信息查看对应的bucket内的文件信息
而且会在当前目录下生成一个名为tmp.xml的文件，包含该bucket下所有文件的信息。

ACL相关功能
^^^^^^^^

使用以下命令设置bucket或object的ACL

.. code::

 coscmd putbucketacl --grant-read --grant-write --grant-full-control
 
 coscmd putobjectacl --grant-read --grant-write --grant-full-control cos_path

若为根帐号设置权限，则输入rootid，若为子帐号设置权限，则需输入rootid/subid，若需要对所有人开发权限，则输入anyone，同类型权限设置多个帐号用逗号隔开

具体细节详见样例

使用以下命令得到bucket或object的ACL

.. code::

 coscmd getbucketacl
 
 coscmd getobjectacl cos_path


简单示例
^^^^^^^^

.. code::

 设置属性
 coscmd config -a ACCESS_ID -s ACCESS_KEY -u 1252448703 -b uploadtest -r cn-south -m 10 -p 5

 上传文件
 coscmd upload file1 file2

 上传文件夹
 coscmd upload -r folder1 folder2

 下载文件
 coscmd download file1 file2

 删除文件
 coscmd delete file1

 删除文件夹
 coscmd delete -r folder1

 新建bucket
 coscmd createbucket

 删除bucket
 coscmd deletebucket

 遍历bucket
 coscmd listbucket

 设置bucket ACL
 coscmd putbucketacl --grant-read anyone,1231,3210232098/345725437 -grant-full-control anyone
 
 设置object ACL

 coscmd putobjectacl --grant-read anyone,1231,3210232098/345725437 --grant-write anyone,1231,3210232098/345725437 -grant-full-control anyone cos_path
 
 得到bucket ACL
 coscmd getbucketacl
 
 得到object ACL
 coscmd getobjectacl

注意事项
^^^^^^^^

配置项建议：

#. max_thread <= 10
#. parts_size <= 10

暂时不支持的功能：

#. 不能删除和下载文件夹
#. 不能删除非空bucket
