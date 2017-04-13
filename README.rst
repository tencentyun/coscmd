


使用文档
========

依赖
--------

操作系统为linux, python版本为2.7，系统安装有pip（可通过yum/apt来安装，包名为python-pip）。


安装
--------

执行如下命令安装:

.. code::

 pip install coscmd -U --nocache

使用方法

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过如下命令来配置。


配置参数

.. code::

 coscmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r cn-south
 
请将参数替换为您的真实id/key/appid/bucket和园区代号。园区为cn-south或者cn-north。


上传文件

使用如下命令上传文件：

.. code::

 coscmd upload ~/t.cpp t1/t.cpp 
 2017-01-18 16:55:32,139 - Init multipart upload ok
 2017-01-18 16:55:32,184 - upload /home/liuchang/t.cpp with  0.00%
 2017-01-18 16:55:32,184 - upload /home/liuchang/t.cpp with 100.00%
 2017-01-18 16:55:32,185 - multipart upload ok
 2017-01-18 16:55:32,226 - complete multipart upload ok
 
如果上传成功，命令行会返回0
