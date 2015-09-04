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

import atexit
import cPickle as pickle
from contextlib import contextmanager
from hashlib import md5
from eventlet import Timeout
import time

from swift.common.utils import normalize_timestamp
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileDeleted, DiskFileNotOpen, DiskFileNoSpace, \
    DiskFileError
from swift.common.swob import multi_range_iterator
from swift.obj.diskfile import METADATA_KEY


class RadosFileSystem(object):
    def __init__(self, ceph_conf, rados_user, rados_pool, **kwargs):
        self._conf = ceph_conf
        self._user = rados_user
        self._pool = rados_pool

        self._rados = None
        self._ioctx = None
        try:
            import rados
        except ImportError:
            rados = None
        self.RADOS = kwargs.get('rados', rados)
        atexit.register(self._shutdown)

    def _get_rados(self, fs):
        if self._rados is None:
            #self._rados = fs.RADOS.Rados(conffile=fs._conf, rados_id=fs._user)
            self._rados = fs.RADOS.Rados(conffile=fs._conf)
            self._rados.connect()
        return self._rados

    def _shutdown(self):
        if self._rados:
            self._rados.shutdown()

    class _radosfs(object):
        def __init__(self, fs, _get_rados):
            self._rados = _get_rados(fs)
            self._fs = fs
            self._pool = fs._pool
            self._ioctx = self._rados.open_ioctx(self._pool)

        def close(self):
            self._ioctx.close()

        def del_object(self, obj):
            try:
                self._ioctx.remove_object(obj)
            finally:
                pass

        def get_metadata(self, obj):
            ret = None
            try:
                ret = pickle.loads(self._ioctx.get_xattr(obj, METADATA_KEY))
            finally:
                return ret

        def put_metadata(self, obj, metadata):
            # Pickle the metadata object and set it on a xattr
            self._ioctx.set_xattr(obj, METADATA_KEY,
                                  pickle.dumps(metadata))

        def put_object(self, obj, metadata):
            self._ioctx.aio_flush()
            self.put_metadata(obj, metadata)

        def size(self, obj):
            size = 0
            try:
                (size, ts) = self._ioctx.stat(obj)
            finally:
                return size

        def create(self, obj, size):
            try:
                self._ioctx.trunc(obj, size)
            except self._fs.RADOS.NoSpace:
                raise DiskFileNoSpace()

        def write(self, obj, offset, data):
            try:
                self._ioctx.write(obj, data, offset)
            except self._fs.RADOS.NoSpace:
                raise DiskFileNoSpace()
            except Exception:
                raise DiskFileError()
            return len(data)

        def read(self, obj, off):
            return self._ioctx.read(obj, offset=off)

        def quarantine(self, obj):
            # There is no way of swift recon monitor to get information
            # of the quarantined file, so better clean it up
            self.del_object(obj)

    def open(self):
        return self._radosfs(self, self._get_rados)

    def get_diskfile(self, device, partition, account,
                     container, obj, **kwargs):
        return DiskFile(self, device, partition, account,
                        container, obj)


class DiskFileWriter(object):
    """
    .. note::
        RADOS based alternative pluggable on-disk backend implementation.

    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for DiskFile's create()
    method.

    :param fs: internal file system object to use
    :param name: standard object name
    """
    def __init__(self, fs, name):
        self._fs = fs
        self._name = name
        self._write_offset = 0

    def write(self, chunk):
        """
        Write a chunk of data.

        :param chunk: the chunk of data to write as a string object
        """
        written = 0
        while written != len(chunk):
            written += self._fs.write(self._name,
                                      self._write_offset + written,
                                      chunk[written:])
        self._write_offset += len(chunk)
        return self._write_offset

    def put(self, metadata):
        """
        Flush all the writes so far and set the metadata

        :param metadata: dictionary of metadata to be written
        """
        metadata['name'] = self._name
        self._fs.put_object(self._name, metadata)

    def commit(self, timestamp):
        pass


class DiskFileReader(object):
    """
    .. note::
        RADOS based alternative pluggable on-disk backend implementation.

    Encapsulation of the read context for servicing GET REST API
    requests. Serves as the context manager object for DiskFile's reader()
    method.

    :param fs: internal filesystem object
    :param name: object name
    :param obj_size: on-disk size of object in bytes
    :param etag: MD5 hash of object from metadata
    :param iter_hook: called when __iter__ returns a chunk
    """
    def __init__(self, fs, name, obj_size, etag, iter_hook=None):
        self._fs = fs.open()
        self._name = name
        self._obj_size = obj_size
        self._etag = etag
        self._iter_hook = iter_hook
        #
        self._iter_etag = None
        self._bytes_read = 0
        self._read_offset = 0
        self._started_at_0 = False
        self._read_to_eof = False
        self._suppress_file_closing = False

    def __iter__(self):
        try:
            if self._read_offset == 0:
                self._started_at_0 = True
            self._read_to_eof = False
            self._iter_etag = md5()
            while True:
                chunk = self._fs.read(self._name, self._read_offset)
                if chunk:
                    if self._iter_etag:
                        self._iter_etag.update(chunk)
                    self._read_offset += len(chunk)
                    yield chunk
                    if self._iter_hook:
                        self._iter_hook()
                else:
                    self._read_to_eof = True
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
        self._read_offset = start
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            self._suppress_file_closing = True
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
        finally:
            self._suppress_file_closing = False
            try:
                self.close()
            except DiskFileQuarantined:
                pass

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        if not ranges:
            yield ''
        else:
            try:
                self._suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self._suppress_file_closing = False
                try:
                    self.close()
                except DiskFileQuarantined:
                    pass

    def _quarantine(self, msg):
        self._fs.quarantine(self._name)

    def _handle_close_quarantine(self):
        if self._read_offset != self._obj_size:
            self._quarantine(
                "Bytes read: %d, does not match metadata: %d" % (
                    self._read_offset, self._obj_size))
        elif self._iter_etag and \
                self._etag != self._iter_etag.hexdigest():
            self._quarantine(
                "ETag %s and file's md5 %s do not match" % (
                    self._etag, self._iter_etag.hexdigest()))

    def close(self):
        """
        Close the file. Will handle quarantining file if necessary.
        """
        self._fs.close()
        try:
            if self._started_at_0 and self._read_to_eof:
                self._handle_close_quarantine()
        except (Exception, Timeout):
            pass


class DiskFile(object):
    """
    .. note::

    RADOS based alternative pluggable on-disk backend implementation.

    Manage object files in RADOS filesystem

    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param iter_hook: called when __iter__ returns a chunk
    :param keep_cache: caller's preference for keeping data read in the cache
    """

    def __init__(self, fs, device, partition, account, container, obj):
        self._name = '/' + '/'.join((device, partition, account,
                                     container, obj))
        self._metadata = None
        self._fs = fs

    def open(self):
        """
        Open the file and read the metadata.

        This method must populate the _metadata attribute.
        :raises DiskFileCollision: on name mis-match with metadata
        :raises DiskFileDeleted: if it does not exist, or a tombstone is
                                 present
        :raises DiskFileQuarantined: if while reading metadata of the file
                                     some data did pass cross checks
        """
        self._fs_inst = self._fs.open()
        self._metadata = self._fs_inst.get_metadata(self._name)
        if self._metadata is None:
            raise DiskFileDeleted()
        self._verify_data_file()
        self._metadata = self._metadata or {}
        return self

    def __enter__(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self

    def __exit__(self, t, v, tb):
        self._fs_inst.close()

    def _quarantine(self, msg):
        self._fs_inst.quarantine(self._name)
        raise DiskFileQuarantined(msg)

    def _verify_data_file(self):
        """
        Verify the metadata's name value matches what we think the object is
        named.

        :raises DiskFileCollision: if the metadata stored name does not match
                                   the referenced name of the file
        :raises DiskFileNotExist: if the object has expired
        :raises DiskFileQuarantined: if data inconsistencies were detected
                                     between the metadata and the file-system
                                     metadata
        """
        try:
            mname = self._metadata['name']
        except KeyError:
            self._quarantine("missing name metadata")
        else:
            if mname != self._name:
                raise DiskFileCollision('Client path does not match path '
                                        'stored in object metadata')
        try:
            x_delete_at = int(self._metadata['X-Delete-At'])
        except KeyError:
            pass
        except ValueError:
            # Quarantine, the x-delete-at key is present but not an
            # integer.
            self._quarantine(
                "bad metadata x-delete-at value %s" % (
                    self._metadata['X-Delete-At']))
        else:
            if x_delete_at <= time.time():
                raise DiskFileNotExist('Expired')
        try:
            metadata_size = int(self._metadata['Content-Length'])
        except KeyError:
            self._quarantine(
                "missing content-length in metadata")
        except ValueError:
            # Quarantine, the content-length key is present but not an
            # integer.
            self._quarantine(
                "bad metadata content-length value %s" % (
                    self._metadata['Content-Length']))

        obj_size = self._fs_inst.size(self._name)
        if obj_size != metadata_size:
            self._quarantine(
                "metadata content-length %s does"
                " not match actual object size %s" % (
                    metadata_size, obj_size))

    def get_metadata(self):
        """
        Provide the metadata for an object as a dictionary.

        :returns: object's metadata dictionary
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metadata

    def read_metadata(self):
        """
        Return the metadata for an object.

        :returns: metadata dictionary for an object
        """
        with self.open():
            return self.get_metadata()

    def reader(self, iter_hook=None, keep_cache=False):
        """
        Return a swift.common.swob.Response class compatible "app_iter"
        object. The responsibility of closing the open file is passed to the
        DiskFileReader object.

        :param iter_hook:
        :param keep_cache:
        """
        if self._metadata is None:
            raise DiskFileNotOpen()

        dr = DiskFileReader(self._fs, self._name,
                            int(self._metadata['Content-Length']),
                            self._metadata['ETag'],
                            iter_hook=iter_hook)
        return dr

    @contextmanager
    def create(self, size=None):
        """
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        fs_inst = None
        try:
            fs_inst = self._fs.open()
            if size is not None:
                fs_inst.create(self._name, size)

            yield DiskFileWriter(fs_inst, self._name)
        finally:
            if fs_inst is not None:
                fs_inst.close()

    def write_metadata(self, metadata):
        """
        Write a block of metadata to an object.
        """
        with self.create() as writer:
            writer.put(metadata)

    def delete(self, timestamp):
        """
        Perform a delete for the given object in the given container under the
        given account.

        :param timestamp: timestamp to compare with each file
        """
        fs_inst = None
        try:
            timestamp = normalize_timestamp(timestamp)
            fs_inst = self._fs.open()
            md = fs_inst.get_metadata(self._name)
            if md and md['X-Timestamp'] < timestamp:
                fs_inst.del_object(self._name)
        finally:
            if fs_inst is not None:
                fs_inst.close()
