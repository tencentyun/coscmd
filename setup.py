from setuptools  import setup,find_packages
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
    license='MIT',
    author='lewzylu',
    author_email='327874225@qq.com',
    description='simple upload command for cos',
    long_description=long_description(),
    #py_module=['cos_upload_cmd','cos_upload_auth','cos_upload_threadpool','cos_upload_client'],
    packages=find_packages(['cos_upload_cmd']),
    install_requires=requirements(),
    entry_points={
        'console_scripts': [
            'cos_upload_cmd=cos_upload_cmd.cos_upload_cmd:_main',
        ],
    }
)
