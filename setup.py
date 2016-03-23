from setuptools import setup, find_packages

readme = open('README.rst').read() + open('CHANGELOG.rst').read()

setup(
    name='aiorq',
    version='0.2.dev1',
    description='asyncio client and server for RQ',
    long_description=readme,
    url='https://github.com/proofit404/aiorq',
    license='LGPL-3',
    author='Artem Malyshev',
    author_email='proofit404@gmail.com',
    packages=find_packages(),
    install_requires=['rq>=0.5', 'aioredis>=0.2', 'click>=3.0'],
    entry_points={
        'console_scripts': [
            'aiorq = aiorq.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development',
    ])
