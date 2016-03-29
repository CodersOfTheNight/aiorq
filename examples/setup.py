from setuptools import setup

setup(name='examples',
      py_modules=['http_client', 'run_example'],
      install_requires=['aiohttp', 'aiorq'])
