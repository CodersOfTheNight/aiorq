#!/bin/bash -e

# Add deadsnakes mirror.

echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu precise main" > /etc/apt/sources.list.d/python.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys DB82666C

# Add redis mirror.

echo "deb http://ppa.launchpad.net/chris-lea/redis-server/ubuntu precise main" > /etc/apt/sources.list.d/redis.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys C7917B12

# Update mirrors list.

apt-get update

# Install redis 3.0.5

apt-get install -y redis-server=3:3.0.5-1chl1~precise1

# Install python 3.4

apt-get install -y python3.4 python3.4-dev

# Install setuptools and pip.

wget https://bootstrap.pypa.io/ez_setup.py -O /tmp/ez_setup.py
python3.4 /tmp/ez_setup.py
easy_install-3.4 pip

# Install tox.

pip install tox
