# -*- coding: utf-8 -*-

__author__ = 'zhangpanpan'
import threading
from zabbixagent.logger import get_logger
from time import time
from zabbixagent.database import Database

class Item(threading.Thread):

    def __init__(self, config, section, shutdown_event, queue):
        super(Item, self).__init__()
        self._config = config
        self._section = section
        self._update_interval = None
        self._keys = None
        self._update_result = dict()
        self._queue = queue
        self._shutdown_event = shutdown_event

        self._fetch_item_keys()
        self._fetch_update_interval()
        self._logger = get_logger()
        self._logger.debug('Setup item %s' % self.__class__.__name__)

    def _fetch_item_keys(self):
        item_keys = self._config.get(self._section, 'item_keys')
        if item_keys:
            keys = eval(item_keys)
            keys = keys if isinstance(keys, (list, tuple)) else (keys,)
            self._keys = set(keys)
        self._assert_have_keys_attribute()

    def _assert_have_keys_attribute(self):
        if not self._keys:
            message = u'Item %s is enabled but no keys defined' % self.__class__.__name__
            raise RuntimeError(message)

    def _fetch_update_interval(self):
        self._update_interval = self._config.get(self._section, 'update_interval')

    def get_name(self):
        return self.__class__.__name__


    def get_update_interval(self):
        return self._update_interval

    def run(self):
        raise NotImplementedError

    def _reset_update_result(self):
        self._update_result = dict()

