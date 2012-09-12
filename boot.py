import time
from ConfigParser import ConfigParser
from path import path
from boto import ec2

EBS_IMAGE='ami-137bcf7a'
INST_IMAGE='ami-097ace60'

def main():
    conn = connect()
    terminate_cluster(conn, 'test')
    start_cluster(conn, 'test')

def terminate_cluster(conn, name):
    for res in conn.get_all_instances(
        filters={'tag:ClusterName': name}):
        for inst in res.instances:
            print 'Terminate %s' % inst
            inst.terminate()

def start_cluster(conn, name):
    server_inst, client_inst = start_instances(conn)
    server_inst.add_tag('Name', '%s-server' % name)
    client_inst.add_tag('Name', '%s-client' % name)
    server_inst.add_tag('ClusterName', name)
    client_inst.add_tag('ClusterName', name)
    write_environ(name, server_inst, client_inst)

def write_environ(name, server_inst, client_inst):
    with open('%s-settings.py' % name, 'w') as fp:
        print >> fp, 'SERVER_PUBLIC=%r' % server_inst.public_dns_name
        print >> fp, 'SERVER_PRIVATE=%r' % server_inst.private_dns_name
        print >> fp, 'CLIENT_PUBLIC=%r' % client_inst.public_dns_name
        print >> fp, 'CLIENT_PRIVATE=%r' % client_inst.private_dns_name

def start_instances(conn):
    server_img = conn.get_image(INST_IMAGE)
    client_img = conn.get_image(EBS_IMAGE)
    res = server_img.run(
        key_name='ec2-arborean',
        instance_type='m1.small')
    server_inst = res.instances[0]
    res = client_img.run(
        key_name='ec2-arborean',
        instance_type='m1.small')
    client_inst = res.instances[0]
    instances = [ server_inst, client_inst ]
    wait_instances(instances)
    return instances
    

def wait_instances(instances):
    while True:
        for i in instances:
            status = i.update()
            print i, status
            if status == 'pending': break
        else:
            break
        time.sleep(5)
    
def connect():
    cp = ConfigParser()
    cp.read([path('~/etc/ec2.conf').expand()])
    conn = ec2.connection.EC2Connection(
        cp.get('ec2', 'access'),
        cp.get('ec2', 'secret'))
    return conn

if __name__ == '__main__':
    main()
