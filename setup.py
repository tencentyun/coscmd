from setuptools import setup, find_packages
from platform import python_version_tuple
from coscmd import cos_global


def requirements():
    with open('requirements.txt', 'r') as fileobj:
        requirements = [line.strip() for line in fileobj]

        version = python_version_tuple()

        if version[0] == 2 and version[1] == 6:
            requirements.append("argparse==1.4.0")
        if version[0] == 3:
            requirements.append("argparse==1.1")
        return requirements


def long_description():
    with open('README.md', 'rb') as fileobj:
        return fileobj.read().decode('utf8')


setup(
    name='coscmd',
    version=cos_global.Version,
    url='https://www.qcloud.com/',
    license='MIT',
    author='lewzylu',
    author_email='327874225@qq.com',
    description='simple command for cos',
    long_description=long_description(),
    packages=find_packages(),
    install_requires=requirements(),
    entry_points={
        'console_scripts': [
            'coscmd=coscmd.cos_cmd:_main',
        ],
    }
)
