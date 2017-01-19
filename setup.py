from setuptools  import setup
# from distutils.core import setup
from platform import python_version_tuple


def requirements():

    with open('requirements.txt', 'r') as fileobj:
        requirements = [line.strip() for line in fileobj]

        version = python_version_tuple()

        if version[0] == 2 and version[1] == 6:
            requirements.append("argparse==1.4.0")
        return requirements

def long_description():
    with open('README.rst', 'r') as fileobj:
        return fileobj.read()

setup(
    name='coscmd',
    version='0.0.6',
    url='https://www.qcloud.com/',
    packages=['coscmd'],
    license='MIT',
    author='liuchang',
    author_email='liuchang0812@gmail.com',
    description='simple command for cos',
    long_description=long_description(),
    entry_points={
        'console_scripts': [
            'coscmd=coscmd.main:_main'
        ],
    },
    install_requires=requirements()
)
