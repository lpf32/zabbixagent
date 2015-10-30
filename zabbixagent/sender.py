# -*- coding: utf-8 -*-

from zabbixagent.logger import get_logger
from time import time
from struct import pack, unpack
from socket import (
    AF_INET,
    SOCK_STREAM,
    error as SocketError,
    getfqdn,
    socket
)
import re
from json import dumps, loads

# Zabbix sender protocol header
ZBX_HEADER = '''ZBXD\1'''
ZBX_RESPONSE = re.compile('Processed:? ([0-9]*);? Failed:? ([0-9]*);? Total:? ([0-9]*);? Seconds spent:?.*', re.IGNORECASE)

class Sender(object):

    def __init__(self, server, port, socket_timeout, send_interval, simulate, database,
                 hostname):
        self._server = server
        self._port = port
        self._sock_timeout = socket_timeout
        self._database = database
        self._send_interval = send_interval
        self._simulate = simulate
        self._process_date = None
        self._force = None
        self._next_send_date = time()
        self._logger = get_logger()
        self._hostname = hostname or getfqdn()

    def send(self, process_date=None, force=False):
        self._store_process_date(process_date)
        self._store_force(force)
        if force or self._sending_data_is_due():
            self._update_next_send_date()
            self._send_pending_data()

    def _store_process_date(self, process_date):
        if not process_date:
            self._process_date = time()
        else:
            self._process_date = process_date

    def _store_force(self, force):
        self._force = force

    def _sending_data_is_due(self):
        return self._process_date >= self._next_send_date

    def _update_next_send_date(self):
        self._next_send_date = time() + self._send_interval

    def _send_pending_data(self):
        try:
            self._try_to_send_pending_data()
        except SocketError, e:
            self._logger.warning('A socket error is occurred when sending items %s' % e)
        except Exception, e:
            self._logger.warning('An error is occurred when sending items %s' % e, exc_info=True)

    def _try_to_send_pending_data(self):
        chunk_size = 200
        items = self._database.query_pending_items()
        self._logger.debug('send items: %s' % items)

        forced = 'forced' if self._force else ''
        self._logger.info('Send a total of %s items to Zabbix server%s' % (len(items), forced))

        while items:
            self._send_items(items[:chunk_size])
            del items[:chunk_size]

        self._logger.info('Items successfully send to Zabbix server')

    def _send_items(self, items):
        try:
            result = self._try_to_send_items(items)
        except Exception:
            raise
        else:
            self._logger.debug('send result: %s' % result)
            self._database.delete_items(items)
            processed, failed, total = self._parse_processed_message(result['info'])
            result = 'Processed %s Faild %s Total %s' % (processed, failed, total)
            if failed:
                self._logger.warning('Processed a chunk of %s items(%s)' % (len(items), result))
            else:
                self._logger.debug('Processed a chunk of %s items(%s)' % (len(items), result))

    def _try_to_send_items(self, items):
        request_data = list()
        for item in items:
            request_item = dict(
                host=self._hostname,
                key=item['key'],
                value=item['value'],
                clock=item['entry_date']
            )
            request_data.append(request_item)

        request = dict(request='sender data', clock=self._process_date, data=request_data)
        request = dumps(request)
        return self._send_request(request)

    def _send_request(self, request):
        self._logger.debug('simulate is %s' % self._simulate)
        if self._simulate:
            self._logger.debug('this is a simulation')
            return dict(info='Processed 0 Failed 0 Total 0 Seconds spent 0')

        request_length = len(request)
        request_header = '%s\0\0\0\0' % pack('i', request_length)

        data_to_send = '%s%s%s' % (ZBX_HEADER, request_header, request)

        sock = socket(AF_INET, SOCK_STREAM)
        sock.settimeout(self._sock_timeout)
        sock.connect((self._server, self._port))

        sock.sendall(data_to_send)

        response_header = sock.recv(5)
        if not response_header == ZBX_HEADER:
            raise ValueError('Invaild response')

        response_data_header = sock.recv(8)
        response_data_header = response_data_header[:4]
        response_len = unpack('i', response_data_header)

        response_raw = sock.recv(response_len)

        sock.close()

        response = loads(response_raw)
        if response['response'] != 'success':
            raise ValueError('Invalid response: %s: %s' % (response['response'], response['info']))

        return response

    def _parse_processed_message(self,result):
        match = ZBX_RESPONSE.match(result)
        try:
            processed, failed, total = map(int, match.groups())
        except Exception:
            processed, failed, total = 0, 0, 0

        return processed, failed, total