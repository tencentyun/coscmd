使用文档
========

依赖
--------

操作系统为linux, python版本为2.7，系统安装有pip（可通过yum/apt来安装，包名为python-pip）。


安装
--------

执行如下命令安装:

.. code::
 
sudo python setup.py install
 

使用方法
--------

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过如下命令来配置。

配置参数
--------

.. code::

 cos_upload_cmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r cn-south -m max_thread -p parts_size

 
请将参数替换为您的真实id/key/appid/bucket和园区代号。园区为cn-south或者cn-north。

请将参数替换为您所需要的本地文件路径，以及cos上存储路径。

max_thread为多线程上传时的最大线程数(默认为2)

parts_size为分块上传的单块大小(单位为M)(默认为1M)

！！！注意：请保证 parts_size * max_thread * 2 <= 你的内存大小

上传速度取决于parts_size * max_thread，但是上限是上行带宽和硬盘读写速度的最大值。

否则可能会导致内存溢出等内存错误。


上传文件
--------

使用如下命令上传文件：

.. code::

 cos_upload_cmd upload ~/t.cpp t1/t.cpp 


 2017-01-18 16:55:32,139 - Init multipart upload ok
 2017-01-18 16:55:32,184 - upload /home/liuchang/t.cpp with  0.00%
 2017-01-18 16:55:32,184 - upload /home/liuchang/t.cpp with 100.00%
 2017-01-18 16:55:32,185 - multipart upload ok
 2017-01-18 16:55:32,226 - complete multipart upload ok
