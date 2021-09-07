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
import configparser
import cgi
from pymongo import MongoClient
from wsgicomm import WINotFoundError
from wsgicomm import WIRedirect


class DC_Module(object):
    """Plugable module for the main :class:`~DCApp` object.

    The download method extending the Data Collections API is registered.
    A connection to the MySQL DB is established as soon as this object is
    created.

    :param dc: MySQL cursor containing the result of a query
    :type dc: :class:`~DCApp`
    :param confFile: File name containing the configuration file to use.
    :type confFile: string

    :platform: Any

    """

    def __init__(self, dc, conffile='../datacoll.cfg'):
        """Constructor from DC_Module."""
        dc.registerAction('GET', ("collections", "*", "members", "*",
                                  "download"), self.memberDownload)

        # We keep a copy of it
        self.__dc = dc

        config = configparser.RawConfigParser()
        here = os.path.dirname(__file__)
        config.read(os.path.join(here, conffile))

        # Read connection parameters
        self.host = config.get('mysql', 'host')
        self.user = config.get('mysql', 'user')
        self.password = config.get('mysql', 'password')
        self.db = config.get('mysql', 'db')
        self.limit = config.getint('mysql', 'limit')

        # TODO Check if this is needed
        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

    def memberDownload(self, environ):
        """Return the actual data file of a collection member.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: Property name and value from a collection member in JSON
            format.
        :rtype: string
        :raises: WIRedirect, WINotFoundError

        """
        # The keep_blank_values=1 is needed to recognize the download key
        # despite that it has no value associated.
        # (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cid = int(splitColl[1])
        except Exception:
            cid = None

        try:
            mid = int(splitColl[3])
        except Exception:
            mid = None

        cursor = self.conn.cursor()
        query = 'select m.pid, m.url from member as m inner join collection '
        query = query + 'as c on m.cid = c.id'

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [cid]

        whereClause.append('m.id = %s')
        sqlParams.append(mid)

        query = query + ' where ' + ' and '.join(whereClause)

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        logging.debug(str(sqlParams))
        try:
            cursor.execute(query, tuple(sqlParams))
        except Exception:
            messDict = {'code': 0,
                        'message': 'Error searching for member %s' % mid}
            message = json.dumps(messDict)
            raise WINotFoundError(message)

        # Get the PID and solve it through Handle
        dbPid, dbUrl = cursor.fetchone()

        if dbPid is not None:
            url = 'http://hdl.handle.net/%s' % dbPid
        else:
            url = dbUrl

        logging.debug('Downloading data from %s' % url)

        cursor.close()
        raise WIRedirect(url=url)
