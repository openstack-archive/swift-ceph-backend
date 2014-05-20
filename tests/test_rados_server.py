# Copyright (c) 2010-2013 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import mock

MOCK_RADOS = mock.Mock(name='rados')
MOCK_RADOS.__name__ = 'rados'
sys.modules['rados'] = MOCK_RADOS

import cStringIO
import unittest
from test.unit.proxy import test_server
from test.unit.proxy.test_server import teardown
from swift_ceph_backend import rados_server


class ObjectNotFound(Exception):
    pass

MOCK_RADOS.ObjectNotFound = ObjectNotFound


class MockIoctx(object):
    def __init__(self):
        self._objs = {}

    def get_xattr(self, key, attr_name):
        if self._objs.get(key) is None:
            raise MOCK_RADOS.ObjectNotFound
        o = self._obj(key, None, None)
        return o['attr']

    def set_xattr(self, key, attr_name, attr):
        self._obj(key, None, attr)

    def _obj(self, name, size, attr=None):
        o = self._objs.get(name)
        if o is None:
            fd = cStringIO.StringIO()
            if attr is None:
                attr = ''
            o = self._objs[name] = {'size': size, 'fd': fd, 'attr': attr}
        else:
            if size is not None:
                o['size'] = size
            if attr is not None:
                o['attr'] = attr

        return o

    def stat(self, key):
        obj = self._obj(key, None)
        return (obj['fd'].tell(), 0)

    def trunc(self, key, size):
        self._obj(key, size)

    def write(self, key, data, offset=0):
        o = self._obj(key, None)
        fd = o['fd']
        if offset < fd.tell():
            fd.seek(offset, os.SEEK_SET)
        fd.write(data)
        return len(data)

    def read(self, key, length=8192, offset=0):
        o = self._obj(key, None)
        fd = o['fd']
        fd.seek(offset, os.SEEK_SET)
        return fd.read(length)

    def aio_flush(self):
        pass

    def close(self):
        pass

    def remove_object(self, key):
        del self._objs[key]


def setup():
    mock_rados_Rados = mock.MagicMock()
    MOCK_RADOS.Rados.return_value = mock_rados_Rados
    mock_rados_Rados.open_ioctx.return_value = MockIoctx()
    test_server.do_setup(rados_server)


class TestController(test_server.TestController):
    pass


class TestProxyServer(test_server.TestProxyServer):
    pass


class TestObjectController(test_server.TestObjectController):
    pass


class TestContainerController(test_server.TestContainerController):
    pass


class TestAccountController(test_server.TestAccountController):
    pass


class TestAccountControllerFakeGetResponse(
        test_server.TestAccountControllerFakeGetResponse):
    pass


if __name__ == '__main__':
    setup()
    try:
        unittest.main()
    finally:
        teardown()
