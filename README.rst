XCOSCMD
#######################

.. image:: https://img.shields.io/pypi/v/coscmd.svg
   :target: https://pypi.org/search/?q=coscmd
   :alt: Pypi
.. image:: https://travis-ci.org/tencentyun/coscmd.svg?branch=master
   :target: https://travis-ci.org/tencentyun/coscmd
   :alt: Travis CI 

介绍
_______

腾讯云COS命令行工具, 目前可以支持Python2.6与Python2.7以及Python3.x。

安装指南
__________

::

    python setup.py install
    cd dist
    sudo easy_install coscmd-1.8.6.3-py3.6.egg # 提前安装easy_install
    ln -s {{PYTHON_DIR}}/bin/coscmd xcoscmd # [可选]为避免和官方coscmd冲突，通过软链使用此版本

或
::
    pip install xcoscmd -i https://pypi.tuna.tsinghua.edu.cn/simple
    或 pip install xcoscmd -i https://pypi.doubanio.com/simple
    或 pip install xcoscmd -i http://mirrors.tencent.com/pypi/simple


改造
__________
上传命令，支持上传同名文件不覆盖，直接跳过

命令::

    xcoscmd -rm ./app /app

上传结果输出优化：上传失败的log红色显示，上传成功的log绿色显示，更加直观的显示上传结果

使用方法
__________

使用coscmd，参照 https://cloud.tencent.com/document/product/436/10976

