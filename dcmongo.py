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

import json
import urllib as ul
import datetime
from bson.objectid import ObjectId

# For the time being these are the capabilities for the datasets
# coming from the user requests.
capabilitiesFixed = {
                     'isOrdered': True,
                     'appendsToEnd': True,
                     'supportsRoles': False,
                     'membershipIsMutable': True,
                     'metadataIsMutable': False
                    }


class DCEncoder(json.JSONEncoder):
    def default(self, obj):
        # Objects lie IDs probably
        if isinstance(obj, ObjectId):
            return str(obj)

        # Bytes to str
        if isinstance(obj, bytes):
            return obj.decode('utf-8')

        # Datetime type to ISO format (str)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        # Otherwise the default behaviour
        return json.JSONEncoder.default(self, obj)


class urlFile(object):
    """Iterable object which retrieves the bitstream pointed by a URL."""

    def __init__(self, url):
        """Create the iterable object.

        :param url: URL to download the data from.
        :type url: string
        """
        self.url = url

    def __iter__(self):
        """Method to iterate on the content."""
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
            print('Exception %s' % str(e))

        raise StopIteration


class Collections(object):
    """Abstraction from the DB storage for a list of Collections."""

    def __init__(self, conn, owner=None, limit=None):
        """Constructor of the list of collections.

        :param conn: datacoll database in MongoDB.
        :type conn: Mongo database
        :param owner: Mail of the owner of the collection.
        :type owner: string
        :param limit: Limit the number of records from the result.
        :type limit: int
        """

        clause = dict()
        # Filter by owner if present in the parameters
        if owner is not None:
            clause['owner'] = owner

        # TODO How to implement this?
        if limit:
            pass

        self.cursor = conn.Collection.find(clause)

    def fetchone(self):
        """Retrieve the next Collection like a cursor."""
        return self.cursor.fetchone()

    def __del__(self):
        """Destructor of the list of Collections."""
        self.cursor.close()


class Members(object):
    """Abstraction from the DB storage for a list of Members."""

    def __init__(self, conn,  collid=None, limit=None):
        """Constructor of the list of Members.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: int
        :param limit: Limit the number of records from the result.
        :type limit: int
        """
        clause = dict()
        # Filter by owner if present in the parameters
        if collid is not None:
            clause['_id'] = ObjectId(collid)

        # TODO How to implement this?
        if limit:
            pass

        self.cursor = conn.Member.find(clause)

    def fetchone(self):
        """Retrieve the next Member like a cursor.

        :returns: The next member of the collection.
        :rtype: :class:`~MemberBase`
        """
        return self.cursor.fetchone()

    def __del__(self):
        """Destructor of the list of Members."""
        self.cursor.close()


class Collection(object):
    """Abstraction from the DB storage for the Collection."""

    def __init__(self, conn, collid=None):
        """Constructor of a Collection object.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: str
        :returns: A collection from the DB based on the given parameters.
        :rtype: :class:`~CollectionBase`
        :raises: Exception
        """
        self.__conn = conn
        # If no filters are given then return an empty object
        if collid is None:
            self.document = dict()
            self._id = None
            return

        # _id must always be a str
        if isinstance(collid, bytes):
            self._id = collid.decode('utf-8')
        else:
            self._id = str(collid)

        self.document = conn.Collection.find_one({'_id': ObjectId(self._id)})

        # If the document do not exist create it in memory first
        if self.document is None:
            raise Exception('Collection %s does not exist!' % self._id)

    def insert(self, document=None):
        """Insert a new collection in the MySQL DB.

        :param document: Collection.
        :type document: dict
        :returns: A new collection.
        :rtype: :class:`~InsertOneResult`
        :raise: Exception
        """
        if document is not None:
            self.document = document

        # TODO What happens if _id is different?
        inserted = self.__conn.Collection.insert_one(self.document)
        self._id = str(inserted.inserted_id)
        return self._id.encode('utf-8')

    def update(self, document=None):
        """Update the fields passed as parameters in the MySQL DB.

        :param document: Collection.
        :type document: dict
        :returns: The updated collection.
        :rtype: :class:`~InsertOneResult`
        :raises: Exception
        """
        if '_id' in document and self._id != str(document['_id']):
            raise Exception('IDs differ!')

        auxdoc = self.__conn.Collection.find_one_and_update({'_id': ObjectId(self._id)},
                                                            document)

        if auxdoc is None:
            document['_id'] = self._id
            inserted = self.__conn.Collection.insert_one(document)
            self._id = str(inserted.inserted_id)

        return self._id

    def delete(self):
        """Delete a Collection from the MySQL DB.

        """
        deleted = self.__conn.Collection.delete_one({'_id': ObjectId(self._id)})

        # Check this. The value must be 1
        if deleted.deleted_count != 1:
            raise Exception('Collection not found!')
        self._id = None
        self.document = None


class Member(object):
    """Abstraction from the DB storage for the Member."""

    def __init__(self, conn, collid=None, memberid=None):
        """Constructor of the Member.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: str
        :param memberid: Member ID.
        :type memberid: str
        :returns: A member from the DB based on the given parameters.
        :rtype: :class:`~MemberBase`
        :raises: Exception
        """
        self.__conn = conn
        if collid is None:
            raise Exception('Empty collection ID!')

        # If no filters are given then return an empty object
        if memberid is None:
            self.document = dict()
            self._id = None
            return

        # _id must always be a str
        if isinstance(collid, bytes):
            self._id = collid.decode('utf-8')
        else:
            self._id = str(collid)

        # _id must always be a str
        if isinstance(memberid, bytes):
            self._id = memberid.decode('utf-8')
        else:
            self._id = str(memberid)

        self.document = conn.Member.find_one({'_id': ObjectId(self._id)})

        # If the document do not exist create it in memory first
        if self.document is None:
            raise Exception('Member %s does not exist!' % self._id)

    def download(self):
        """Download a Member from the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        """
        cursor = conn.cursor()
        query = 'select m.pid, m.url from member as m inner join collection '
        query = query + 'as c on m.cid = c.id'

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [self.collid]

        whereClause.append('m.id = %s')
        sqlParams.append(self.memberid)

        query = query + ' where ' + ' and '.join(whereClause)

        try:
            cursor.execute(query, tuple(sqlParams))
        except:
            messDict = {'code': 0,
                        'message': 'Error searching for member %s' % self.memberid}
            message = json.dumps(messDict)
            raise Exception(message)

        # Get the PID and solve it through Handle
        dbPid, dbUrl = cursor.fetchone()

        if dbPid is not None:
            url = 'http://hdl.handle.net/%s' % dbPid
        else:
            url = dbUrl

        cursor.close()
        return url

    def delete(self):
        """Delete a Member from the MySQL DB.

        """
        deleted = self.__conn.Member.delete_one({'_id': ObjectId(self._id)})

        # Check this. The value must be 1
        if deleted.deleted_count != 1:
            raise Exception('Member not found!')
        self._id = None
        self.document = None

    def insert(self, document=None):
        """Insert a new Member in the MySQL DB.

        :param document: Member.
        :type document: dict
        :returns: A member from the DB based on the given parameters.
        :rtype: :class:`~InsertOneResult`
        :raises: Exception
        """
        if document is not None:
            self.document = document

        # TODO What happens if _id is different?
        inserted = self.__conn.Member.insert_one(self.document)
        self._id = str(inserted.inserted_id)
        return self._id.encode('utf-8')

    def update(self, document=None):
        """Update the fields passed as parameters in the MySQL DB.

        :param document: Member.
        :type document: dict
        :returns: The updated Member.
        :rtype: :class:`~InsertOneResult`
        :raises: Exception
        """
        if '_id' in document and self._id != str(document['_id']):
            raise Exception('IDs differ!')

        auxdoc = self.__conn.Member.find_one_and_update({'_id': ObjectId(self._id)},
                                                        document)

        if auxdoc is None:
            document['_id'] = self._id
            inserted = self.__conn.Member.insert_one(document)
            self._id = str(inserted.inserted_id)

        return self._id


class JSONFactory(object):
    """Iterable object which provides JSON version of different objects.

     For instance, :class:`~Member` or :class:`~Collection`.

    :param cursor: MySQL cursor containing the result of a query
    :type cursor: MySQLdb.cursors.Cursor
    :param objType: Class which must be used to create the objects returned
    :type objType: type
    """

    def __init__(self, cursor, objType):
        """Constructor of the JSONFactory."""
        self.cursor = cursor
        self.objType = objType
        # 0: Header must be sent; 1: Send 1st collection; 2: Send more items
        # 3: Headers have been closed and StopIteration should be raised
        self.status = 0
        self.content_type = 'application/json'

    def __iter__(self):
        """Iterative method."""
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
            return b'{"contents": ['

        # Headers have been closed. Raise StopIteration
        if self.status == 3:
            raise StopIteration

        # Load a record
        reg = self.cursor.fetchone()
        if reg is None:
            # There are no records, close cursor and headers, set status = 3
            self.status = 3
            return b']}'

        if self.status == 1:
            self.status = 2
            # Send first collection
            return reg.toJSON().encode()
        else:
            # Status=2 send a separator and a collection
            return (', %s' % reg.toJSON()).encode()

    __next__ = next
