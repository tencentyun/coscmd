COSCMD使用文档
========

更新
--------
1.1.0 增加上传文件夹功能

1.1.1 修改上传部分失败的总结信息

依赖
--------

操作系统为linux, python版本为2.7，系统安装有pip（可通过yum/apt来安装，包名为python-pip）。


安装
--------

在该项目根目录下执行如下命令安装:

.. code::
 
 sudo python setup.py install


使用方法
--------

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过如下命令来配置。


配置参数
!!!!!!!!

.. code::

 sudo coscmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r region -m max_thread -p parts_size


请将参数替换为您的真实id/key/appid/bucket和园区代号,园区(region)为cn-south或者cn-north。

max_thread为多线程上传时的最大线程数(默认为5)

parts_size为分块上传的单块大小(单位为M)(默认为1M)


上传文件
!!!!!!!!

使用如下命令上传文件：

.. code::

 sudo coscmd upload localpath cospath 

请将参数替换为您所需要的本地文件路径(localpath)，以及cos上存储的路径(cospath)。

如果本地文件路径是一个文件夹，则会将文件夹以cospath的名字上传

简单示例
!!!!!!!!

.. code::

 coscmd config -a AKID15IsskiBQKTZbAo6WhgcBqVls9SmuG00 -s ciivKvnnrMvSvQpMAWuIz12pThGGlWRW -u 1252448703 -b uploadtest -r cn-south -m 10 -p 5
 coscmd upload 1.txt 1.txt

.. code::
 
 2017-06-25 09:51:19,138 - config parameter:
 appid: 1252448703, region: cn-south, bucket: uploadtest, part_size: 1, max_thread: 5
 2017-06-25 09:51:39,207 - Init multipart upload ok
 2017-06-25 09:51:39,207 - upload ans.csv with 0.00%
 2017-06-25 09:51:41,223 - upload ans.csv with 25.00%
 2017-06-25 09:51:41,844 - upload ans.csv with 50.00%
 2017-06-25 09:51:42,016 - upload ans.csv with 75.00%
 2017-06-25 09:51:42,549 - upload ans.csv with 100.00%
 2017-06-25 09:51:42,549 - multipart upload ok
 2017-06-25 09:51:46,604 - complete multipart upload ok


注意事项
!!!!!!!!

该版本为测试版
max_thread <= 10
parts_size <= 10
