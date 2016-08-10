#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from .errors import LivestatusError

import socket
import json


__all__ = ['Query', 'Socket']


class Query(object):
    def __init__(self, conn, resource):
        self._conn = conn
        self._resource = resource
        self._columns = []
        self._filters = []

    def call(self):
        try:
            data = bytes(str(self), 'utf-8')
        except TypeError:
            data = str(self)
        return self._conn.call(data)

    __call__ = call

    def __str__(self):
        request = 'GET %s\nResponseHeader: fixed16' % (self._resource)
        if self._columns and any(self._columns):
            request += '\nColumns: %s' % (' '.join(self._columns))
        if self._filters:
            for filter_line in self._filters:
                request += '\nFilter: %s' % (filter_line)
        request += '\nOutputFormat: json\nColumnHeaders: on\n'
        return request

    def columns(self, *args):
        self._columns = args
        return self

    def filter(self, filter_str):
        self._filters.append(filter_str)
        return self


class Socket(object):
    def __init__(self, peer):
        self.peer = peer

    def __getattr__(self, name):
        return Query(self, name)

    def call(self, request):
        try:
            if len(self.peer) == 2:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(self.peer)
            s.send(request)
            s.shutdown(socket.SHUT_WR)
            rawdata = s.makefile().read()
            s.close()
        except Exception as err:
            s.close()
            raise LivestatusError("Failed to connect to Livestatus.  Reason: %s" % (err))
        else:
            if not rawdata:
                raise LivestatusError("Livestatus service returned no data.")
            data = self.validateHeader(rawdata)
            data = json.loads(data)
            return [dict(zip(data[0], value)) for value in data[1:]]

    def validateHeader(self, rawdata):
        header = rawdata[0:16]
        if header[0:3] == "200":
            data = rawdata[16:]
            return data
        else:
            raise LivestatusError("The Livestatus query contained an error. Reason: %s" % (rawdata[16:]))
