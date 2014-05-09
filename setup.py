from setuptools import setup

setup(
    name = 'swift-ceph-backend',
    version = '0.1',
    description = 'Ceph backend for OpenStack Swift',
    license = 'Apache License (2.0)',
    packages = ['swift_ceph_backend'],
    classifiers = [
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)'],
    install_requires = ['swift', ],
    entry_points = {
        'paste.app_factory': ['rados_object = swift_ceph_backend.rados_server:app_factory'],
    },
)
