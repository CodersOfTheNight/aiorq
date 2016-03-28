from setuptools import setup

setup(name='examples',
      py_modules=['http_client', 'enqueue_coroutine'],
      install_requires=['aiohttp', 'aiorq'])
