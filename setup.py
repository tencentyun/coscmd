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
    name='cos_upload_cmd',
    version='0.1.9',
    url='https://www.qcloud.com/',
    packages=['cos_upload_cmd'],
    license='MIT',
    author='lewzylu',
    author_email='327874225@qq.com',
    description='simple upload command for cos',
    long_description=long_description(),
    entry_points={
        'console_scripts': [
            'cos_upload_cmd=cos_upload_cmd.cos_upload_cmd:_main'
        ],
    },
    install_requires=requirements(),
    py_modules=['cos_upload_cmd']
)
