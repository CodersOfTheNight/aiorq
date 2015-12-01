from setuptools import setup, find_packages

setup(
    name='aiorq',
    version='0.1.dev1',
    description='asyncio client and server for RQ',
    url='https://github.com/proofit404/aiorq',
    license='LGPL-3',
    author='Artem Malyshev',
    author_email='proofit404@gmail.com',
    packages=find_packages(),
    install_requires=['rq', 'aioredis'])
