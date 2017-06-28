COSCMD使用文档
========

更新
--------
1.1.0 增加上传文件夹功能

1.1.1 修改上传部分失败的总结信息

1.2.0 增加了文件夹上传进度条

1.3.0 增加了下载文件的功能

依赖
--------

操作系统为linux, python版本为2.7，系统安装有pip（可通过yum/apt来安装，包名为python-pip）。


安装
--------

在该项目根目录下执行如下命令安装:

.. code::
 
 python setup.py install


使用方法
--------

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过如下命令来配置。


配置参数
!!!!!!!!

.. code::

 coscmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r region -m max_thread -p parts_size


请将参数替换为您的真实id/key/appid/bucket和园区代号,园区(region)为cn-south或者cn-north。

max_thread为多线程上传时的最大线程数(默认为5)

parts_size为分块上传的单块大小(单位为M)(默认为1M)


上传文件(夹)
!!!!!!!!

使用如下命令上传文件：

.. code::

 coscmd upload localpath cospath 

请将参数替换为您所需要的本地文件路径(localpath)，以及cos上存储的路径(cospath)。

如果本地文件路径是一个文件夹，则会将文件夹以cospath的名字上传


下载文件
!!!!!!!!

使用如下命令上传文件：

.. code::

 coscmd download localpath cospath 

请将参数替换为您所需要的本地存储路径(localpath)，以及需要下载的cos上文件的路径(cospath)。


简单示例
!!!!!!!!

.. code::

 设置属性
 coscmd config -a AKKTZbAo6WhgcBqVls9SmuG0ID15IsskiBQ0 -s ciivKvnnrMvSvQpMAWuIz12pThGGlWRW -u 1252448703 -b uploadtest -r cn-south -m 10 -p 5

 上传文件
 coscmd upload file1 file2

 上传文件夹
 coscmd upload folder1 folder2

 下载文件
 coscmd download file1 file2


注意事项
!!!!!!!!

该版本为测试版
max_thread <= 10
parts_size <= 10
