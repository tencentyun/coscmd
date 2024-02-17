# COSCMD

[![pypi][pypi-img]][pypi-url]
[![travis][travis-img]][travis-url]

[pypi-img]:https://img.shields.io/pypi/v/coscmd.svg
[pypi-url]:https://pypi.org/search/?q=coscmd
[travis-img]:https://travis-ci.org/tencentyun/coscmd.svg?branch=master
[travis-url]:https://travis-ci.org/tencentyun/coscmd

## 介绍
_______

腾讯云COS命令行工具, 最新版本支持Python3.x, 1.8.x版本支持Python2.6/Python2.7。

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

