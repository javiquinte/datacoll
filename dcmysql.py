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
import datetime
import urllib as ul
from collections import namedtuple

# For the time being these are the capabilities for the datasets
# coming from the user requests.
capabilitiesFixed = {
                     'isOrdered': True,
                     'appendsToEnd': True,
                     'supportsRoles': False,
                     'membershipIsMutable': True,
                     'metadataIsMutable': False
                    }


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


class CollectionBase(namedtuple('CollectionBase', ['collid', 'pid', 'name', 'mail',
                                                   'ts', 'restrictedtotype',
                                                   'rule'])):
    """Namedtuple representing a :class:`~Collection`.

    It includes a method to return its JSON version.
       collid: collection ID (int)
       pid: uuid or pid identifying the collection
       name: name of the collection
       mail: mail address of the owner
       ts: creation of the collection
       restrictedtotype: Type restriction for the members of this collection
       rule: Expression with wildcards to compare with all collection names

    :platform: Any
    """

    __slots__ = ()

    def toJSON(self):
        """Return the JSON version of this collection.

        :returns: This collection in JSON format.
        :rtype: string
        """
        auxCap = capabilitiesFixed.copy()
        auxCap['restrictedtotype'] = self.restrictedtotype

        # FIXME Capabilities should be later separated between (im)mutable
        interVar = ({
                     'collid': self.collid,
                     'pid': self.pid,
                     'name': self.name,
                     'creation': self.ts,
                     'capabilities': auxCap,
                     'rule': self.rule,
                     'properties': {'ownership': self.mail,
                                    'license': 'CC-BY',
                                    'hasAccessRestrictions': False,
                                    'memberOf': ()
                                    }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class Collections(object):
    """Abstraction from the DB storage for a list of Collections."""

    def __init__(self, conn, owner=None, limit=None):
        """Constructor of the list of collections.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param owner: Mail of the owner of the collection.
        :type owner: string
        :param limit: Limit the number of records from the result.
        :type limit: int
        """
        self.cursor = conn.cursor()
        query = 'select c.id, c.pid, c.name, u.mail, ts, d.name, c.rule from '
        query = query + 'collection as c inner join user as u on c.owner = '
        query = query + 'u.id left join datatype as d on '
        query = query + 'c.restrictedtotype = d.id'

        whereClause = list()
        sqlParams = list()

        # Filter by owner if present in the parameters
        if owner is not None:
            whereClause.append('u.mail = %s')
            sqlParams.append(owner)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        self.cursor.execute(query, tuple(sqlParams))

    def fetchone(self):
        """Retrieve the next Collection like a cursor."""
        reg = self.cursor.fetchone()
        # If there are no records
        if reg is None:
            return None

        # Create a CollectionBase
        return CollectionBase(*reg)

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
        self.cursor = conn.cursor()

        # Check if the collection is generated by a rule
        query = 'select rule from collection where id = %s'
        self.cursor.execute(query, (collid,))
        rule = self.cursor.fetchone()[0]

        query = 'select m.cid, m.id, m.pid, m.location, m.checksum, d.name, '
        query = query + 'm.dateadded from member as m left join datatype as d '
        query = query + 'on m.datatype = d.id '

        whereClause = list()
        sqlParams = list()

        if rule is not None:
            query = query + 'inner join collection as c on m.cid = c.id '
            whereClause.append('c.name LIKE %s')
            sqlParams.append(rule)

        if ((rule is None) and (collid is not None)):
            whereClause.append('m.cid = %s')
            sqlParams.append(collid)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        self.cursor.execute(query, tuple(sqlParams))

    def fetchone(self):
        """Retrieve the next Member like a cursor.

        :returns: The next member of the collection.
        :rtype: :class:`~MemberBase`
        """
        reg = self.cursor.fetchone()
        # If there are no records
        if reg is None:
            return None

        # Create a MemberBase
        return MemberBase(*reg)

    def __del__(self):
        """Destructor of the list of Members."""
        self.cursor.close()


class Collection(CollectionBase):
    """Abstraction from the DB storage for the Collection."""

    __slots__ = ()

    def __new__(cls, conn, collid=None, pid=None):
        """Constructor of a Collection object.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: int
        :param pid: PID of the collection.
        :type pid: string
        :returns: A collection from the DB based on the given parameters.
        :rtype: :class:`~CollectionBase`
        """
        # If no filters are given then return an empty object
        if ((collid is None) and (pid is None)):
            self = super(Collection, cls).__new__(cls, None, None, None, None,
                                                  None, None, None)
            return self

        cursor = conn.cursor()
        query = 'select c.id, c.pid, c.name, u.mail, ts, d.name, c.rule from '
        query = query + 'collection as c inner join user as u on '
        query = query + 'c.owner = u.id left join datatype as d on '
        query = query + 'c.restrictedtotype = d.id '

        whereClause = list()
        sqlParams = list()

        if collid is not None:
            whereClause.append('c.id = %s')
            sqlParams.append(collid)
        else:
            whereClause.append('c.pid = %s' if pid is not None
                               else 'c.pid is %s')
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

    def insert(self, conn, name=None, owner=None, pid=None,
               restrictedtotype=None, rule=None):
        """Insert a new collection in the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param name: Name of the collection.
        :type name: string
        :param owner: Mail of the owner of the collection.
        :type owner: string
        :param pid: PID of the added collection.
        :type pid: string
        :param restrictedtotype: Datatype of the members of the collection.
        :type restrictedtotype: string
        :param rule: Pattern with wildcards to compare to collection names.
        :type rule: string
        :returns: A new collection.
        :rtype: :class:`~CollectionBase`
        :raise: Exception
        """
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

        # Insert only if the datatype does not exist yet
        query = 'insert into datatype (name) select * from (select %s) as tmp '
        query = query + 'where not exists '
        query = query + '(select id from datatype where name=%s) limit 1'
        sqlParams = [restrictedtotype, restrictedtotype]

        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        # Read the ID from datatype
        query = 'select id from datatype where name = %s'
        sqlParams = [restrictedtotype]

        cursor.execute(query, tuple(sqlParams))
        # Read datatype ID
        did = cursor.fetchone()[0]

        query = 'insert into collection (pid, name, owner, restrictedtotype, '
        query = query + 'rule) values (%s, %s, %s, %s, %s)'
        sqlParams = [pid, name, uid, did, rule]
        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        whereClause = list()
        sqlParams = list()

        query = 'select id, pid, name, %s, ts, %s, rule from collection'
        sqlParams.extend([owner, restrictedtotype])

        whereClause.append('name = %s' if name is not None else 'name is %s')
        sqlParams.append(name)

        whereClause.append('pid = %s' if pid is not None else 'pid is %s')
        sqlParams.append(pid)

        whereClause.append('owner = %s' if uid is not None else 'owner is %s')
        sqlParams.append(uid)

        query = query + ' where ' + ' and '.join(whereClause)

        cursor.execute(query, tuple(sqlParams))
        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            raise Exception('Collection not inserted')

        self = super(Collection, self).__new__(type(self), *coll)
        return self

    def update(self, conn, name=None, owner=None, pid=None,
               restrictedtotype=None, rule=None):
        """Update the fields passed as parameters in the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param name: Name of the collection.
        :type name: string
        :param owner: Mail of the owner of the collection.
        :type owner: string
        :param pid: PID of the added collection.
        :type pid: string
        :param restrictedtotype: Datatype of the members of the collection.
        :type restrictedtotype: string
        :param rule: Pattern with wildcards to be compared to collection names.
        :type rule: string
        :returns: The updated collection.
        :rtype: :class:`~CollectionBase`
        :raise: Exception
        """
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

        # Insert only if the datatype does not exist yet
        query = 'insert into datatype (name) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from datatype '
        query = query + 'where name=%s) limit 1'
        sqlParams = [restrictedtotype, restrictedtotype]

        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        # Read the ID from datatype
        query = 'select id from datatype where name = %s'
        sqlParams = [restrictedtotype]

        cursor.execute(query, tuple(sqlParams))
        # Read datatype ID
        did = cursor.fetchone()[0]

        query = 'update collection set pid=%s, owner=%s, ts=DEFAULT, ' + \
            'restrictedtotype=%s, name=%s, rule=%s where id=%s'
        sqlParams = [pid, uid, did, name, rule, self.id]
        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        query = 'select id, pid, name, %s, ts, %s, rule from collection '
        query = query + 'where id = %s'
        cursor.execute(query, (owner, restrictedtotype, self.id))
        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            raise Exception('Collection not updated')

        self = super(Collection, self).__new__(type(self), *coll)
        return self

    def delete(self, conn):
        """Delete a Collection from the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        """
        cursor = conn.cursor()
        query = 'delete from collection where id = %s'
        cursor.execute(query, (self.collid, ))
        cursor.close()
        conn.commit()


class MemberBase(namedtuple('Member', ['collid', 'memberid', 'pid', 'location',
                                       'checksum', 'datatype', 'dateadded'])):
    """Namedtuple representing a :class:`~Member`.

    It includes methods to to return the JSON version of the member.
       memberid: int identifying the member
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
        # FIXME See that datatype is harcoded. This must be actually queried
        # from the member but it has still not been added to the table columns
        interVar = ({
                     'memberid': self.memberid,
                     'pid': self.pid,
                     'location': self.location,
                     'datatype': self.datatype,
                     'checksum': self.checksum,
                     'mappings': {
                                   'index': self.memberid,
                                   'dateAdded': self.dateadded
                                 }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class Member(MemberBase):
    """Abstraction from the DB storage for the Member."""

    __slots__ = ()

    def __new__(cls, conn, collid=None, memberid=None, pid=None, location=None):
        """Constructor of the Member.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: int
        :param memberid: Member ID.
        :type memberid: int
        :param pid: PID of the collection.
        :type pid: string
        :param location: URL where collection can be found.
        :type location: string
        :returns: A member from the DB based on the given parameters.
        :rtype: :class:`~MemberBase`
        :raises: Exception
        """
        # If no filters are given then return an empty object
        if ((collid is None) and (memberid is None)):
            self = super(Member, cls).__new__(cls, None, None, None, None,
                                              None, None, None)
            return self

        cursor = conn.cursor()

        query = 'select m.cid, m.id, m.pid, m.location, m.checksum, d.name, '
        query = query + 'm.dateadded from member as m left join datatype as d '
        query = query + 'on m.datatype = d.id '

        whereClause = list()
        sqlParams = list()

        if collid is not None:
            whereClause.append('m.cid = %s')
            sqlParams.append(collid)

        if memberid is not None:
            whereClause.append('m.id = %s')
            sqlParams.append(memberid)
        else:
            if pid is not None:
                whereClause.append('m.pid = %s')
                sqlParams.append(pid)

            if location is not None:
                whereClause.append('m.location = %s')
                sqlParams.append(location)

        if len(sqlParams):
            query = query + ' where ' + ' and '.join(whereClause)

        cursor.execute(query, tuple(sqlParams))

        # Read one member because an ID is given. Check that there is
        # something to return (result set not empty)
        member = cursor.fetchone()
        cursor.close()

        if member is None:
            raise Exception('Member not found!')

        self = super(Member, cls).__new__(cls, *member)
        return self

    def download(self, conn):
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

    def delete(self, conn):
        """Delete a Member from the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        """
        cursor = conn.cursor()
        query = 'delete from member where cid = %s and id = %s'
        cursor.execute(query, (self.collid, self.memberid, ))
        cursor.close()
        conn.commit()

    def insert(self, conn, collid, pid=None, location=None, checksum=None,
               datatype=None, index=None):
        """Insert a new Member in the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param collid: Collection ID.
        :type collid: int
        :param pid: PID of the collection.
        :type pid: string
        :param location: URL where collection can be found.
        :type location: string
        :param checksum: Checksum of the data file.
        :type checksum: string
        :param datatype: Data type of the resource specified by the Member.
        :type datatype: string
        :param index: Member position (index) within the collection.
        :type index: int
        :returns: A member from the DB based on the given parameters.
        :rtype: :class:`~MemberBase`
        :raises: Exception
        """
        cursor = conn.cursor()

        # Either pid or location should have a valid value
        if ((pid is None) and (location is None)):
            msg = 'Either PID or location should have a valid non empty value.'
            raise Exception(msg)

        if datatype is None:
            datatypeID = None
        else:
            query = 'select id from datatype where name = %s'
            cursor.execute(query, (datatype,))
            datatypeID = cursor.fetchone()

            if datatypeID is None:
                query = 'insert into datatype (name) values (%s)'
                cursor.execute(query, (datatype,))
                conn.commit()
                # Retrieve the ID which was recently created
                query = 'select id from datatype where name = %s'
                cursor.execute(query, (datatype,))

                datatypeID = cursor.fetchone()

        query = 'insert into member (cid, pid, location, checksum,datatype,id)'
        if index is None:
            query = query + ' select %s, %s, %s, %s, %s, coalesce(max(id),0)+1'
            query = query + ' from member where cid = %s'
            sqlParams = [collid, pid, location, checksum, datatypeID, collid]
        else:
            query = query + ' values (%s, %s, %s, %s, %s, %s)'
            sqlParams = [collid, pid, location, checksum, datatypeID, index]

        try:
            cursor.execute(query, tuple(sqlParams))
            conn.commit()
        except:
            conn.commit()
            cursor.close()
            msg = 'Creation of member raised an error! Was it already present?'
            raise Exception(msg)

        query = 'select m.cid, m.id, m.pid, m.location, m.checksum, d.name, '
        query = query + 'm.dateadded from member as m left join datatype as d '
        query = query + 'on m.datatype = d.id'

        whereClause = ['m.cid=%s']
        sqlParams = [collid]

        if pid is not None:
            whereClause.append('m.pid = %s')
            sqlParams.append(pid)

        if location is not None:
            whereClause.append('m.location = %s')
            sqlParams.append(location)

        if len(sqlParams):
            query = query + ' where ' + ' and '.join(whereClause)

        cursor.execute(query, tuple(sqlParams))

        # Read one member because an ID is given. Check that there is
        # something to return (result set not empty)
        member = cursor.fetchone()

        cursor.close()
        self = super(Member, self).__new__(type(self), *member)
        return self

    def update(self, conn, memberid=None, pid=None, location=None, checksum=None,
               datatype=None):
        """Update the fields passed as parameters in the MySQL DB.

        :param conn: Connection to the MySQL DB.
        :type conn: MySQLdb.connections.Connection
        :param id: ID of the collection.
        :type id: int
        :param pid: PID of the collection.
        :type pid: string
        :param location: URL where collection can be found.
        :type location: string
        :param checksum: Checksum of the data file.
        :type checksum: string
        :param datatype: Data type of the resource specified by the Member.
        :type datatype: string
        :returns: A member from the DB based on the given parameters.
        :rtype: :class:`~MemberBase`
        :raises: Exception
        """
        setClause = list()
        sqlParams = list()

        if memberid is not None:
            setClause.append('id = %s')
            sqlParams.append(memberid)

        if pid is not None:
            setClause.append('pid = %s')
            sqlParams.append(pid)

        if location is not None:
            setClause.append('location = %s')
            sqlParams.append(location)

        if checksum is not None:
            setClause.append('checksum = %s')
            sqlParams.append(checksum)

        if datatype is not None:
            setClause.append('datatype = %s')
            sqlParams.append(datatype)

        # If there is nothing to change
        if not len(sqlParams):
            return self

        cursor = conn.cursor()
        query = 'update member set ' + ', '.join(setClause)
        query = query + ' where cid = %s and id = %s'
        sqlParams.extend([self.collid, self.memberid])

        cursor.execute(query, tuple(sqlParams))
        conn.commit()

        # Retrieve the updated record from the DB
        query = 'select m.cid, m.id, m.pid, m.location, m.checksum, d.name, '
        query = query + 'm.dateadded from member as m inner join collection '
        query = query + 'as c on m.cid = c.id left join datatype as d '
        query = query + 'on m.datatype = d.id where m.cid = %s and m.id = %s'

        cursor.execute(query, (self.collid, self.memberid if memberid is None else memberid))
        memb = cursor.fetchone()
        cursor.close()
        if memb is None:
            raise Exception('Member not updated')

        self = super(Member, self).__new__(type(self), *memb)
        return self


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
