#!/bin/bash -e

# Add deadsnakes mirror.

echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu precise main" > /etc/apt/sources.list.d/python.list
wget --quiet -O - "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xFF3997E83CD969B409FB24BC5BB92C09DB82666C" | apt-key add -

# Add redis mirror.

echo "deb http://ppa.launchpad.net/chris-lea/redis-server/ubuntu precise main" > /etc/apt/sources.list.d/redis.list
wget --quiet -O - "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x136221EE520DDFAF0A905689B9316A7BC7917B12" | apt-key add -

# Update mirrors list.

apt-get update

# Install redis.

apt-get install -y redis-server=3:3.0.7-1chl1~precise1

# Install python.

apt-get install -y python3.4 python3.4-dev

# Install setuptools and pip.

wget https://bootstrap.pypa.io/ez_setup.py -O /tmp/ez_setup.py
python3.4 /tmp/ez_setup.py
easy_install-3.4 pip

# Install tox.

pip install tox
