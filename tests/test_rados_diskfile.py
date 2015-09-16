# vim: tabstop=4 shiftwidth=4 softtabstop=4
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import mock
from mock import call, patch
import cPickle as pickle
import unittest
from hashlib import md5
from swift.common.exceptions import DiskFileQuarantined, \
    DiskFileCollision, DiskFileNotOpen

from swift_ceph_backend.rados_diskfile import RadosFileSystem
from swift_ceph_backend.rados_diskfile import METADATA_KEY


class TestRadosDiskFile(unittest.TestCase):
    def setUp(self):
        super(TestRadosDiskFile, self).setUp()
        self.mock_rados = mock.MagicMock(name='rados')
        self.Rados = self.mock_rados.Rados.return_value
        self.ioctx = self.Rados.open_ioctx.return_value
        self.rados_ceph_conf = 'xxx-ceph.conf'
        self.rados_name = 'xxx-rados-name'
        self.rados_pool = 'xxx-rados-pool'
        self.device = 'device'
        self.partition = '0'
        self.account = 'account'
        self.container = 'container'
        self.obj_name = 'myobject'
        self.logger = mock.Mock()
        self.rdf = RadosFileSystem(self.rados_ceph_conf,
                                   self.rados_name,
                                   self.rados_pool,
                                   self.logger, 
                                   rados=self.mock_rados)
        self.df = self.rdf.get_diskfile(self.device,
                                        self.partition,
                                        self.account,
                                        self.container,
                                        self.obj_name)

    def tearDown(self):
        super(TestRadosDiskFile, self).tearDown()
        self.mock_rados.reset_mock()
        self.Rados.reset_mock()
        self.ioctx.reset_mock()
        del self.rdf
        del self.df

    def _obj_name(self):
        return '/' + '/'.join((self.device, self.partition,
                               self.account, self.container, self.obj_name))

    def _assert_if_rados_not_opened(self):
        self.mock_rados.Rados.assert_called_once_with(
            conffile=self.rados_ceph_conf, rados_id=self.rados_name)
        self.Rados.connect.assert_called_once_with()
        self.Rados.open_ioctx.assert_called_once_with(self.rados_pool)

    def _assert_if_rados_not_closed(self):
        self.ioctx.close.assert_called_once_with()

    def _assert_if_rados_opened(self):
        assert((self.mock_rados.Rados.call_count == 0) and
               (self.Rados.connect.call_count == 0) and
               (self.Rados.open_ioctx.call_count == 0))

    def _assert_if_rados_closed(self):
        assert((self.ioctx.close.call_count == 0) and
               (self.Rados.shutdown.call_count == 0))

    def _assert_if_rados_opened_closed(self):
        assert((self.mock_rados.Rados.call_count > 0) and
               (self.Rados.connect.call_count > 0) and
               (self.Rados.open_ioctx.call_count > 0))
        assert((self.Rados.connect.call_count > 0) and
               (self.Rados.open_ioctx.call_count ==
                self.Rados.connect.call_count))

    def test_df_open_1(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        with self.df.open():
            pass
        self._assert_if_rados_not_opened()
        self.ioctx.get_xattr.assert_called_once_with(self._obj_name(),
                                                     METADATA_KEY)
        #self._assert_if_rados_not_closed()

    def test_df_open_invalid_name(self):
        meta = {'name': 'invalid', 'Content-Length': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        try:
            self.df.open()
        except DiskFileCollision:
            success = True
        except Exception:
            pass
        finally:
            assert(success)

    def test_df_open_invalid_content_length(self):
        meta = {'name': self._obj_name(), 'Content-Length': 100}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        try:
            self.df.open()
        except DiskFileQuarantined:
            success = True
        except Exception:
            pass
        finally:
            assert(success)

    def test_df_notopen_check(self):
        success = False
        try:
            with self.df:
                pass
        except DiskFileNotOpen:
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened()
            self._assert_if_rados_closed()

    def test_df_notopen_get_metadata(self):
        success = False
        try:
            self.df.get_metadata()
        except DiskFileNotOpen:
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened()
            self._assert_if_rados_closed()

    def test_df_get_metadata(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        ret_meta = None
        try:
            with self.df.open():
                ret_meta = self.df.get_metadata()
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            assert(ret_meta == meta)
            self._assert_if_rados_not_opened()
            #self._assert_if_rados_not_closed()

    def test_df_read_metadata(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        ret_meta = None
        try:
            ret_meta = self.df.read_metadata()
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            assert(ret_meta == meta)
            self._assert_if_rados_not_opened()
            #self._assert_if_rados_not_closed()

    def test_df_notopen_reader(self):
        success = False
        try:
            self.df.reader()
        except DiskFileNotOpen:
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened()
            self._assert_if_rados_closed()

    def test_df_open_reader_1(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        try:
            with self.df.open():
                self.df.reader()
        except KeyError:
            success = True
            pass
        finally:
            assert(success)
            self._assert_if_rados_not_opened()
            #self._assert_if_rados_not_closed()

    def test_df_open_reader_2(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0, 'ETag': ''}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        try:
            with self.df.open():
                rdr = self.df.reader()
            rdr.close()
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened_closed()

    def test_df_reader_iter_invalid_cont_len(self):
        etag = md5()
        fcont = '123456789'
        etag.update(fcont)

        meta = {'name': self._obj_name(), 'Content-Length': len(fcont),
                'ETag': etag.hexdigest()}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (len(fcont), 0)
        success = False
        try:
            with self.df.open():
                rdr = self.df.reader()
                num_chunks = 0

                self.ioctx.read.return_value = fcont
                for chunk in rdr:
                    num_chunks += 1
                    assert(chunk == fcont)
                    if num_chunks == 3:
                        self.ioctx.read.return_value = None
                assert(num_chunks == 3)
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened_closed()

            # check read calls
            call_list = [call.read(self._obj_name(), offset=0),
                         call.read(self._obj_name(), offset=len(fcont)),
                         call.read(self._obj_name(), offset=(2 * len(fcont))),
                         call.read(self._obj_name(), offset=(3 * len(fcont)))]
            self.ioctx.assert_has_calls(call_list)
            self.ioctx.remove_object.assert_called_once_with(self._obj_name())

    def test_df_reader_iter_invalid_etag(self):
        etag = md5()
        fcont = '123456789'
        etag.update(fcont)

        meta = {'name': self._obj_name(), 'Content-Length': (3 * len(fcont)),
                'ETag': etag.hexdigest()}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = ((len(fcont) * 3), 0)
        success = False
        try:
            with self.df.open():
                rdr = self.df.reader()
                num_chunks = 0

                self.ioctx.read.return_value = fcont
                for chunk in rdr:
                    num_chunks += 1
                    assert(chunk == fcont)
                    if num_chunks == 3:
                        self.ioctx.read.return_value = None
                assert(num_chunks == 3)
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened_closed()

            # check read calls
            call_list = [call.read(self._obj_name(), offset=0),
                         call.read(self._obj_name(), offset=len(fcont)),
                         call.read(self._obj_name(), offset=(2 * len(fcont))),
                         call.read(self._obj_name(), offset=(3 * len(fcont)))]
            self.ioctx.assert_has_calls(call_list)
            self.ioctx.remove_object.assert_called_once_with(self._obj_name())

    def test_df_reader_iter_all_ok(self):
        etag = md5()
        fcont = '123456789'
        etag.update(fcont)
        etag.update(fcont)
        etag.update(fcont)

        meta = {'name': self._obj_name(), 'Content-Length': (3 * len(fcont)),
                'ETag': etag.hexdigest()}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = ((3 * len(fcont)), 0)
        success = False
        try:
            with self.df.open():
                rdr = self.df.reader()
                num_chunks = 0

                self.ioctx.read.return_value = fcont
                for chunk in rdr:
                    num_chunks += 1
                    assert(chunk == fcont)
                    if num_chunks == 3:
                        self.ioctx.read.return_value = None
                assert(num_chunks == 3)
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened_closed()

            # check read calls
            call_list = [call.read(self._obj_name(), offset=0),
                         call.read(self._obj_name(), offset=len(fcont)),
                         call.read(self._obj_name(), offset=(2 * len(fcont))),
                         call.read(self._obj_name(), offset=(3 * len(fcont)))]
            self.ioctx.assert_has_calls(call_list)

            # if everything is perfect, the object will not be deleted
            assert(self.ioctx.remove_object.call_count == 0)

    def test_df_reader_iter_range(self):
        etag = md5()
        fcont = '0123456789'
        etag.update(fcont)

        meta = {'name': self._obj_name(), 'Content-Length': len(fcont),
                'ETag': etag.hexdigest()}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (len(fcont), 0)
        success = False
        try:
            with self.df.open():
                rdr = self.df.reader()
                num_chunks = 0

                def ioctx_read(obj_name, length=8192, offset=0):
                    assert(obj_name == self._obj_name())
                    return fcont[offset:]

                self.ioctx.read = ioctx_read
                for chunk in rdr.app_iter_range(1, 8):
                    num_chunks += 1
                    assert(chunk == '1234567')
                assert(num_chunks == 1)
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_opened_closed()

            assert(self.ioctx.remove_object.call_count == 0)

    def test_df_writer_1(self):
        with self.df.create():
            pass
        assert(self.ioctx.trunc.call_count == 0)
        self._assert_if_rados_not_opened()
        #self._assert_if_rados_not_closed()

        with self.df.create(500):
            pass
        self.ioctx.trunc.assert_called_once_with(self._obj_name(), 500)

    def test_df_writer_write(self):
        fcont = '0123456789'

        writes = []

        def mock_write(self, obj, offset, data):
            writes.append((data, offset))
            return 2

        with patch('swift_ceph_backend.rados_diskfile.'
                   'RadosFileSystem._radosfs.write', mock_write):
            with self.df.create() as writer:
                assert(writer.write(fcont) == len(fcont))

            check_list = [
                (fcont, 0),
                (fcont[2:], 2),
                (fcont[4:], 4),
                (fcont[6:], 6),
                (fcont[8:], 8)]
            assert(writes == check_list)
            self._assert_if_rados_not_opened()
            #self._assert_if_rados_not_closed()

    def test_df_writer_put(self):
        meta = {'Content-Length': 0,
                'ETag': ''}

        with self.df.create() as writer:
            writer.put(meta)

        old_metadata = pickle.dumps(meta)
        ca = self.ioctx.set_xattr.call_args
        check_1 = call(self._obj_name(), METADATA_KEY, old_metadata)

        assert(ca == check_1)
        assert(meta['name'] == self._obj_name())
        self._assert_if_rados_not_opened()
        #self._assert_if_rados_not_closed()

    def test_df_write_metadata(self):
        meta = {'Content-Length': 0,
                'ETag': ''}
        self.df.write_metadata(meta)

        old_metadata = pickle.dumps(meta)
        ca = self.ioctx.set_xattr.call_args
        check_1 = call(self._obj_name(), METADATA_KEY, old_metadata)

        assert(ca == check_1)
        assert(meta['name'] == self._obj_name())
        self._assert_if_rados_not_opened()
        #self._assert_if_rados_not_closed()

    def test_df_delete(self):
        meta = {'name': self._obj_name(), 'Content-Length': 0,
                'X-Timestamp': 0}
        self.ioctx.get_xattr.return_value = pickle.dumps(meta)
        self.ioctx.stat.return_value = (0, 0)
        success = False
        try:
            self.df.delete(1)
            success = True
        except Exception:
            pass
        finally:
            assert(success)
            self._assert_if_rados_not_opened()
            #self._assert_if_rados_not_closed()
            self.ioctx.remove_object.assert_called_once_with(self._obj_name())


if __name__ == '__main__':
    unittest.main()
