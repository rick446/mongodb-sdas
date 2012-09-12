from path import path as pathpy
from fabric.api import *
from fabric.contrib.console import confirm

env.key_filename = 'ec2-arborean.pem'
CLUSTER = {}

def cluster(name):
    global CLUSTER
    CLUSTER = ns = {'NAME': name}
    with open('%s-settings.py' % name) as fp:
        exec fp in ns
    env.roledefs['server'] = [ 'ubuntu@' + ns['SERVER_PUBLIC'] ]
    env.roledefs['client'] = [ 'ubuntu@' + ns['CLIENT_PUBLIC'] ]

def uname():
    run('uname -a')

@roles('server')
def prepare_server():
    sudo('apt-get install -y mongodb-server')
    put('mongodb.conf', '/etc/mongodb.conf', use_sudo=True)
    sudo('service mongodb restart')

@roles('server')
def prepare_server_22():
    put('10gen.list', '/etc/apt/sources.list.d', use_sudo=True)
    sudo('apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10')
    sudo('apt-get update')
    sudo('apt-get install -y mongodb-10gen')
    put('mongodb.conf', '/etc/mongodb.conf', use_sudo=True)
    sudo('service mongodb restart')

@roles('client')
def prepare_client():
    sudo('apt-get install -y python-dev python-virtualenv mongodb-clients')
    run('virtualenv sdas')
    run('sdas/bin/pip install pymongo')
    put('writer.py', 'writer.py')

@roles('client')
def run_client(runtype):
    try:
        run('sdas/bin/python writer.py mongodb://%s:27017 %s' % (
                CLUSTER['SERVER_PRIVATE'], runtype))
    except KeyboardInterrupt:
        get('times.csv', 'times-%s-%s.csv' % (CLUSTER['NAME'], runtype))
        raise
    
@roles('client')
def get_times():
    get('times.csv', 'times-%s.csv' % (CLUSTER['NAME']))
