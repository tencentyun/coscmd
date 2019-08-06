COSCMD
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

使用pip安装 ::

    pip install -U coscmd

手动安装::

    python setup.py install
    cd dist
    sudo easy_install coscmd-1.8.6.3-py3.6.egg # 提前安装easy_install
    ln -s {{PYTHON_DIR}}/bin/coscmd xcoscmd # [可选]为避免和官方coscmd冲突，通过软链使用此版本

使用方法
__________

使用coscmd，参照 https://cloud.tencent.com/document/product/436/10976

