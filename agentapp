#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from ConfigParser import SafeConfigParser
from optparse import OptionParser
import logging
import logging.config
import threading
import pwd
import errno
from signal import signal, SIGINT, SIGTERM
from zabbixagent.database import Database
from zabbixagent.controller import ItemProcessingController
from Queue import Queue
from zabbixagent.sender import Sender

class AgentApp(object):

    def __init__(self):
        self._base_dir = None
        self._items = list()
        self._options = None
        self._config = SafeConfigParser()
        self._pid_file_path = None
        self._logger = None
        self._config_files = None
        self._shutdown_event = threading.Event()
        self._database = None
        self._sender = None
        self._queue = Queue()

    def setup(self):
        self._setup_base_dir()
        self._setup_options()
        self._setup_config()
        self._setup_pidfile_path()
        self._check_already_running()
        self._write_pidfile()
        self._setup_logging()
        self._setup_signal_handler()
        self._setup_database()
        self._setup_sender()
        self._setup_items()

    def _setup_base_dir(self):
        self._base_dir = os.path.abspath(os.path.dirname(__file__))
        os.chdir(self._base_dir)

    def _setup_options(self):
        option_parser = OptionParser()
        option_parser.add_option('-c', dest='config',
                                 default='%s/config/zabbixagent.conf' % self._base_dir,
                                 help=u'configuration file')

        option_parser.add_option('-d', action='store_true', dest='daemonize',
                                 default=False,
                                 help=u'daemonize')

        self._options = option_parser.parse_args()[0]

    def _setup_config(self):
        if not os.path.exists(self._options.config):
            raise RuntimeError('configuration file is not exist')
        config_path, config_filename = os.path.split(self._options.config)
        local_config_filename = u"%s-local%s" % os.path.splitext(config_filename)
        local_config_path = os.path.join(config_path, local_config_filename)
        self._config_files = [self._options.config, local_config_path]
        self._config.read(self._config_files)

    def _setup_uid(self):
        if self._config.has_option('main', 'user'):
            name = self._config.get('main', 'user')
            if name:
                uid = pwd.getpwnam(name)[2]
                os.setuid(uid)

    def _setup_pidfile_path(self):
        self._pid_file_path = self._config.get('main', 'pid_file_path')

    def _check_already_running(self):
        if self._is_service_running():
            raise RuntimeError(u'already running')

    def _is_service_running(self):
        if os.path.exists(self._pid_file_path):
            pid_file = open(self._pid_file_path,'r')
            pid = pid_file.read().strip()
            if pid:
                try:
                    pid = int(pid)
                except Exception:
                    return False

                try:
                    os.kill(pid, 0)
                except OSError,e:
                    if e.errno == errno.ESRCH:
                        return False
                return True
        return False

    def _write_pidfile(self):
        if self._pid_file_path:
            pid_file = open(self._pid_file_path,'w')
            pid = os.getpid()
            pid_file.write(str(pid))
            pid_file.close()

    def _setup_signal_handler(self):
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self._logger.info(u'Received signal %s' % signum)
        if signum in (SIGINT, SIGTERM):
            self._shutdown()
        else:
            raise RuntimeError(u'Unhandled signal received')

    def _setup_logging(self):
        logging.config.fileConfig(self._config_files)
        self._logger = logging.getLogger('zabbixagent')
        self._logger.info('Application start up')

    def _shutdown(self):
        self._logger.info('Initiating shutdown')
        self._shutdown_event.set()

    def _setup_sender(self):
        server = self._config.get('zabbix', 'server')
        port = self._config.getint('zabbix', 'port')
        socket_timeout = self._config.getint('zabbix', 'socket_timeout')
        send_interval = self._config.getint('zabbix', 'send_interval')
        hostname = self._config.get('zabbix', 'hostname')
        simulate = self._config.getboolean('zabbix', 'simulate')
        self._sender = Sender(
            server,
            port,
            socket_timeout,
            send_interval,
            simulate,
            self._database,
            hostname
        )

    def _setup_database(self):
        database_name = self._config.get('database', 'name')
        self._database = Database(database_name)
        self._database.open()

    def _setup_items(self):
        sections = self._config.sections()
        for section in sections:
            if section.startswith('item_'):
                self._setup_item(section)

    def _setup_item(self, section):
        enable = self._config.get(section, 'enable')
        class_name = self._config.get(section, 'class')
        if enable:
            item_name = section[5:]
            item = self._factor_item(item_name, class_name, section)
            self._items.append(item)

    def _factor_item(self, item_name, class_name, section):
        module_name = 'zabbixagent.items.%s' % item_name
        module = __import__(module_name, globals(), locals(), ['items'], 0)
        item_class = getattr(module, class_name)
        return item_class(self._config, section, self._shutdown_event, self._queue)

    def start(self):
        try:
            self._try_to_start()
        except Exception, e:
            self._logger.error(u'An error occurred: %s' % e, exc_info=True)
            try:
                self.shutdown()
                self._teardown()
            except Exception:
                pass
            return 1
        else:
            return 0

    def _try_to_start(self):
        item_process = ItemProcessingController(self._config, self._shutdown_event,
                                                self._items, self._database, self._sender, self._queue)

        item_process.start()

        self._teardown()

    def _teardown(self):
        self._shutdown_sender()
        self._shutdown_database()
        self._shutdown_logging()

    def _shutdown_sender(self):
        pass

    def _shutdown_database(self):
        self._database.close()

    def _shutdown_logging(self):
        self._logger.info(u'Shutdown')
        logging.shutdown()







if __name__ == '__main__':
    a = AgentApp()
    a.setup()

    a.start()