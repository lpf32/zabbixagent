#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'zhangpanpan'

from zabbixagent.logger import get_logger
from sqlite3 import connect


DB_INIT_STATEMENT = '''
    VACUUM;
    CREATE TABLE IF NOT EXISTS 'item_values'
    (
        `id` INTEGER PRIMARY KEY,
        `key`   TEXT,
        `value` TEXT,
        `entry_date` INTEGER
    );
'''

class Database(object):

    def __init__(self, database):
        self._conn = None
        self._database = database
        self._logger = get_logger()

    def open(self):
        try:
            self._conn = connect(self._database, isolation_level=None)
        except Exception:
            self._logger.error('An error occurs when connecting the database %s' % self._database)
            raise RuntimeError('An error occurs when connecting the database %s' % self._database)
        else:
            cursor = self._conn.cursor()
            cursor.executescript(DB_INIT_STATEMENT)

    def close(self):
        try:
            self._conn.close()
        except Exception,e:
            self._logger.warning(u'An error occuured while closeing the database: %s ' % e, exc_info=True)

    def insert_values(self, values):
        items = list()
        entry_date = values['entry_date']
        for key, value in values.items():
            if key == 'entry_date':
                continue
            item = (key, value, entry_date)
            items.append(item)

        cursor = self._conn.cursor()
        cursor.executemany(
            'INSERT INTO `item_values` (`key`, `value`, `entry_date`) VALUES (?,?,?)', items
        )

    def query_pending_items(self):
        cursor = self._conn.cursor()
        cursor.execute('SELECT `id`, `key`, `value`, `entry_date` FROM `item_values`;')
        result = cursor.fetchall()
        items = list()
        for id_, key, value, entry_date in result:
            item = dict(id=id_, key=key, value=value, entry_date=entry_date)
            items.append(item)

        return items

    def delete_items(self, items):
        item_ids = ','.join(map(lambda x: str(x['id']), items))
        cursor = self._conn.cursor()
        cursor.execute('DELETE FROM `item_values` WHERE id IN (%s);' % item_ids)