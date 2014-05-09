Ceph backend for Openstack Swift
================================

Installation
------------

1. Install the rados object server:

    sudo python setup.py install

2. Modify your object-server.conf to use the new object server:

    [app:object-server]
    use = egg:swift_ceph_backend#rados_object

3. Set the user and pool for Ceph in the [DEFAULT] section:

    [DEFAULT] 
    rados_user = swift
    rados_pool = swift
