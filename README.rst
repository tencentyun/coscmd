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

工具的使用前需要配置！你可以直接编辑~/.cos.conf，也可以通过下面的命令来配置。


配置参数
^^^^^^^^

.. code::

 coscmd config -a youraccessid -s yoursecretkey -u appid -b bucketname -r region -m max_thread -p parts_size


请将参数替换为您的真实id/key/appid/bucket和园区代号,园区(region)为cn-south或者cn-north。

max_thread为多线程上传时的最大线程数(默认为5)

parts_size为分块上传的单块大小(单位为M)(默认为1M)

或者直接修改~/.cos.conf文件，下面是一个例子

.. code::

  [common]
 access_id = AChT4ThiXAbpBDEFGhT4ThiXAbpHIJK
 secret_key = WE54wreefvds3462refgwewerewr
 appid = 1251000577
 bucket = ABC
 region = cn-south
 max_thread = 5
 part_size = 1



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



简单示例
^^^^^^^^

.. code::

 设置属性
 coscmd config -a ACCESS_ID -s ACCESS_KEY -u 1252448703 -b uploadtest -r cn-south -m 10 -p 5

 上传文件
 coscmd upload bbb/A.txt aaa/B.txt

 上传文件夹
 coscmd upload -r aaa/folder1 bbb/folder2

 下载文件
 coscmd download aaa/B.txt bbb/A.txt

 删除文件
 coscmd delete aaa/B.txt

 删除文件夹
 coscmd delete -r bbb/folder2

注意事项
^^^^^^^^
暂时不支持文件夹下载功能

配置项建议：

#. max_thread <= 10
#. parts_size <= 10
