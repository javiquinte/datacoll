#!/usr/bin/env python
#
# Data Collection WS - prototype
#
# (c) 2016 Javier Quinteros, GEOFON team
# <javier@gfz-potsdam.de>
#
# ----------------------------------------------------------------------

"""Data Collection WS - prototype

   :Platform:
       Linux
   :Copyright:
       GEOFON, GFZ Potsdam <geofon@gfz-potsdam.de>
   :License:
       GNU General Public License v3

.. moduleauthor:: Javier Quinteros <javier@gfz-potsdam.de>, GEOFON, GFZ Potsdam
"""

import MySQLdb
import configparser
import logging
import json
import datetime
from collections import namedtuple

class Collection(namedtuple('Collection', ['id', 'mail', 'ts'])):
    """Namedtuple representing a Collection.

    It includes methods to to return the JSON version f the collection.
       id: uuid or pid identifying the collection
       mail: mail address of the owner
       ts: creation of the Collection

    :platform: Any

    """

    __slots__ = ()

    def toJSON(self):
        interVar = ({'id': self.id,
                     'creation': self.ts,
                     'capabilities': {'isOrdered': False,
                                      'supportRoles': False,
                                      'membershipIsMutable': False,
                                      'metadataIsMutable': False
                                      },
                     'properties': {'ownership': {'owner': (self.mail)},
                                    'license': '',
                                    'hasAccessRestrictions': False,
                                    'memberOf': (),
                                    'siblings': {'next': {}, 'previous': {}}
                                    }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class CollJSONIter(object):
    def __init__(self, cursor):
        self.cursor = cursor
        # 0: Header must be sent; 1: Send 1st collection; 2: Send more items
        # 3: Headers have been closed and StopIteration should be raised
        self.status = 0

    def __iter__(self):
        return self

    def next(self):
        # Send headers
        if self.status == 0:
            self.status = 1
            return '{"contents": ['

        # Headers have been closed. Raise StopIteration
        if self.status == 3:
            raise StopIteration

        # Load a record
        reg = self.cursor.fetchone()
        if reg is None:
            # There are no records, close cursor and headers, set status = 3
            self.cursor.close()
            self.status = 3
            return ']}'

        coll = Collection._make(reg)
        if self.status == 1:
            # Send first collection
            return reg.toJSON()
        else:
            # Send a separator and a collection
            return ', %s' % reg.toJSON()


class DC_Module(object):
    def __init__(self, confFile='../datacoll.cfg'):
        dc.registerAction("/collections", self.collections)

        # We keep a copy of it
        self.__dc = dc

        config = configparser.RawConfigParser()
        config.read(confFile)

        # Set logging verbosity
        verbo = config.get('Service', 'verbosity')
        # 'WARNING' is the default value
        verboNum = getattr(logging, verbo.upper(), 30)
        logging.basicConfig(level=verboNum)

        # Read connection parameters
        self.host = config.get('mysql', 'host')
        self.user = config.get('mysql', 'user')
        self.password = config.get('mysql', 'password')
        self.db = config.get('mysql', 'db')
        self.limit = config.get('mysql', 'limit')

        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

    def collections(self):
        itC = self.getCollections()
        for c in itC:

    def getCollections(self, filterOwner=None):
        cursor = self.conn.cursor()
        query = 'select id, mail, ts from collection as c inner join user as u'
        query = '%s on c.owner = u.uid' % query

        if filterOwner is not None:
            query = '%s where u.mail = "%s"' % (query, filterOwner)

        if self.limit:
            query = '%s limit %s' % (query, self.limit)

        logging.debug(query)
        cursor.execute(query)
        return CollIter(cursor)
