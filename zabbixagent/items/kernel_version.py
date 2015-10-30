# -*- coding: utf-8 -*-
__author__ = 'zhangpanpan'

from zabbixagent.items.base import Item
from time import time, sleep

KERNEL_VERSION_PATH = '/Users/zhangpanpan/osrelease'


class KernelVersion(Item):

    def run(self):
        while not self._shutdown_event.isSet():
            try:
                if 'system.kernel.version' in self._keys:
                    self._update_result['system.kernel.version'] = self.get_kernel_version()

                if self._update_result:
                    self._update_result['entry_date'] = time()
            except Exception,e:
                self._reset_update_result()
                raise
            else:
                if self._update_result:
                    self._queue.put(self._update_result)
                    self._logger.debug('put %s in queue' % self._update_result)
                    self._reset_update_result()
            sleep(float(self._update_interval))

    def get_kernel_version(self):
        kernel_version_file = open(KERNEL_VERSION_PATH)
        kernel_version = kernel_version_file.read().strip()
        kernel_version_file.close()

        return kernel_version