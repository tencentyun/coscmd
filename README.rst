# COSCMD
#######################

.. image:: https://img.shields.io/pypi/v/coscmd.svg
   :target: https://pypi.org/search/?q=coscmd
   :alt: Pypi
.. image:: https://travis-ci.org/tencentyun/coscmd.svg?branch=master
   :target: https://travis-ci.org/tencentyun/coscmd
   :alt: Travis CI 

## 介绍
_______

腾讯云COS命令行工具, 目前可以支持 Python2.6 与 Python2.7 以及 Python3.x。

## 在 Docker 上使用

```shell
docker run --rm -it tencentcom/tencentyun-coscmd --version
docker run --rm -it tencentcom/tencentyun-coscmd -h
```

## 在 Coding-CI 上使用

```yaml
master:
  push:
  - stages:
    - name: run with tencentyun-coscmd
      image: tencentcom/tencentyun-coscmd
      commands: |
        coscmd --version
        coscmd -h
```

## 安装指南
__________

### 使用pip安装 ::

    pip install -U coscmd

### 手动安装::

    python setup.py install

### 使用方法
__________

使用coscmd，参照 https://cloud.tencent.com/document/product/436/10976

