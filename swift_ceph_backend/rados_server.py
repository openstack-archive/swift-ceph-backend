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

""" Rados Object Server for Swift """

from __future__ import with_statement
import os
from swift import gettext_ as _

from eventlet import Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout

from swift.common.http import is_success
from swift.obj import server

from swift_ceph_backend.rados_diskfile import RadosFileSystem


class ObjectController(server.ObjectController):
    """
    Implements the WSGI application for the Swift Rados Object Server.
    """

    def setup(self, conf):
        ceph_conf = conf.get("rados_ceph_conf", None)
        rados_user = conf.get("rados_user", None)
        rados_pool = conf.get("rados_pool", None)
        self._filesystem = RadosFileSystem(ceph_conf, rados_user, rados_pool,
                                           self.logger)

    def get_diskfile(self, device, partition, account, container, obj,
                     **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a given
        REST API.
        """
        return self._filesystem.get_diskfile(device, partition, account,
                                             container, obj, **kwargs)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice, policy,
                     logger_thread_locals=None):
        """
        Sends or saves an async update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        :param objdevice: device name that the object is in
        """
        headers_out['user-agent'] = 'object-server %s' % os.getpid()
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = http_connect(ip, port, contdevice, partition, op,
                                        full_path, headers_out)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed: %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        # FIXME: For now don't handle async updates

    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        pass


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
