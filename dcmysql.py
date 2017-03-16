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

import os
import logging
import json
import datetime
import configparser
import urllib2 as ul
import cgi
import MySQLdb
from collections import namedtuple

# For the time being these are the capabilities for the datasets
# coming from the user requests.
capabilitiesFixed = {'isOrdered': False,
                     'restrictedToType': "miniSEED",
                     'appendsToEnd': True,
                     'supportsRoles': False,
                     'membershipIsMutable': True,
                     'metadataIsMutable': False
                    }


class urlFile(object):
    def __init__(self, url):
        self.url = url

    def __iter__(self):
        blockSize = 1024 * 1024

        req = ul.Request(self.url)
        try:
            u = ul.urlopen(req)
            buf = u.read(blockSize)
            while len(buf):
                # Read first block of data
                yield buf
                buf = u.read(blockSize)
        except Exception as e:
            logging.error('Exception %s' % str(e))

        raise StopIteration


class CollectionBase(namedtuple('CollectionBase', ['id', 'pid', 'mail', 'ts'])):
    """Namedtuple representing a :class:`~Collection`.

    It includes a method to return its JSON version.
       id: collection ID (int)
       pid: uuid or pid identifying the collection
       mail: mail address of the owner
       ts: creation of the collection

    :platform: Any

    """

    __slots__ = ()


class Collection(CollectionBase):
    """Abstraction from the DB storage for the Collection."""

    __slots__ = ()

    def __new__(cls, conn, collID=None, pid=None):
        # If no filters are given then return an empty object
        if ((collID is None) and (pid is None)):
            return self

        cursor = conn.cursor()
        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id'

        whereClause = list()
        sqlParams = list()

        if collID is not None:
            whereClause.append('c.id = %s')
            sqlParams.append(collID)

        if pid is not None:
            whereClause.append('c.pid = %s')
            sqlParams.append(pid)

        if len(sqlParams):
            query = query + ' where ' + ' and '.join(whereClause)
        cursor.execute(query, tuple(sqlParams))

        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            raise Exception('Collection not found')

        self = super(Collection, cls).__new__(cls, *coll)
        return self

    def update(self, conn, owner=None, pid=None):
        cursor = conn.cursor()
        # Insert only if the user does not exist yet
        query = 'insert into user (mail) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from user where mail=%s) '
        query = query + 'limit 1'
        sqlParams = [owner, owner]

        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        # Read the ID from user
        query = 'select id from user where mail = %s'
        sqlParams = [owner]

        cursor.execute(query, tuple(sqlParams))

        # Read user ID
        uid = cursor.fetchone()[0]

        query = 'update collection set pid = %s, owner = %s, ts=DEFAULT where id = %s'
        sqlParams = [pid, uid, collID]
        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        query = 'select id, pid, %s, ts from collection where id = %s'
        cursor.execute(query, (owner, self.id))
        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            raise Exception('Collection not updated')

        self = super(Collection, cls).__new__(cls, *coll)
        return self

    def delete(self, conn):
        cursor = conn.cursor()
        query = 'delete from collection where id = %s'
        cursor.execute(query, (self.id, ))
        cursor.close()
        conn.commit()

    def toJSON(self):
        """Return the JSON version of this collection.

        :returns: This collection in JSON format
        :rtype: string
        """
        # FIXME Capabilities should be later separated between (im)mutable
        interVar = ({'id': self.id,
                     'pid': self.pid,
                     'creation': self.ts,
                     'capabilities': capabilitiesFixed,
                     'properties': {'ownership': self.mail,
                                    'license': '',
                                    'hasAccessRestrictions': False,
                                    'memberOf': ()
                                    }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class Member(namedtuple('Member', ['id', 'pid', 'location', 'checksum',
                                   'datatype', 'dateadded'])):
    """Namedtuple representing a :class:`~Member`.

    It includes methods to to return the JSON version of the member.
       id: int identifying the member
       pid: a global PID resolvable via handle.net
       location: a URL where the data can be download
       checksum: checksum of the data to check its validity
       datatype: data type of the member
       dateadded: date and time when the member was added to the collection

    :platform: Any

    """
    __slots__ = ()

    def toJSON(self):
        """Return the JSON version of this collection member.

        :returns: This Member in JSON format
        :rtype: string
        """
        # FIXME See that the datatype is harcoded. This must be actually queried
        # from the member but it has still not been added to the table columns
        interVar = ({'id': self.id,
                     'pid': self.pid,
                     'location': self.location,
                     'datatype': self.datatype,
                     'checksum': self.checksum,
                     'mappings': {
                                   'index': self.id,
                                   'dateAdded': self.dateadded
                                 }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class CollJSONIter(object):
    """Iterable object capable of creating JSON chunks representing different
    types of objects. For instance, :class:`~Member` or :class:`~Collection`.

    :param cursor: MySQL cursor containing the result of a query
    :type cursor: MySQLdb.cursors.Cursor
    :param objType: Class which must be used to create the objects returned
    :type objType: type

    :platform: Any

    """

    def __init__(self, cursor, objType):
        self.cursor = cursor
        self.objType = objType
        # 0: Header must be sent; 1: Send 1st collection; 2: Send more items
        # 3: Headers have been closed and StopIteration should be raised
        self.status = 0
        self.content_type = 'application/json'

    def __iter__(self):
        return self

    def next(self):
        """Return the next object.

        Tuples are read from the cursor and returned in JSON format. A veriable
        "status" is defined to track in which state are we. Meanings are:
        0 = Header must be sent ('{"contents": [').
        1 = Send 1st element.
        2 = Send the rest of the elements.
        3 = Headers have been closed and StopIteration should be raised.

        :raises: StopIteration

        """

        # Send headers
        if self.status == 0:
            self.status = 1
            return '{"contents": ['

        # Headers have been closed. Raise StopIteration
        if self.status == 3:
            raise StopIteration

        # Load a record
        reg = self.cursor.fetchone()
        logging.debug(str(reg))
        if reg is None:
            # There are no records, close cursor and headers, set status = 3
            self.cursor.close()
            self.status = 3
            return ']}'

        JSONFactory = self.objType._make(reg)
        if self.status == 1:
            self.status = 2
            # Send first collection
            return JSONFactory.toJSON()
        else:
            # Status=2 send a separator and a collection
            return ', %s' % JSONFactory.toJSON()
