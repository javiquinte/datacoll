# ------------------------------------------------------------
# Author      : massimo.fares@ingv.it
# LastDate    : (01/03/2016)
#
# ------------------------------------------------------------
#
# Simple iRODS data access object

import os
import logging
import json
import irods
from irods.models import DataObject
from irods.session import iRODSSession

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

# from irods.meta import iRODSMeta
# from irods.models import Collection
# from irods.models import Resource
# from irods.models import User
# from irods.models import DataObjectMeta
# from irods.models import CollectionMeta
# from irods.models import ResourceMeta
# from irods.models import UserMeta
# from irods.column import Column, Keyword
# from irods.results import ResultSet


class irodsFile(object):
    """Manage the access to a file stored in iRODS."""

    def __init__(self, session, filename):
        """Constructor of the irodsFile class."""
        self.sess = session
        self.fullPath = filename
        self.filename = os.path.basename(filename)
        self.content_type = 'application/octet-stream'

    def __iter__(self):
        """Iterator to read the file."""
        blockSize = 500 * 1024

        try:
            obj = self.sess.data_objects.get(self.fullPath)
            with obj.open('r') as f:
                # Read first block of data
                buff = f.read(blockSize)
                while len(buff):
                    yield buff
                    buff = f.read(blockSize)
        except Exception as e:
            logging.error('Exception', str(e))

        raise StopIteration


class irodsInterface(object):
    """Manage the access to the iRODS federation.

    Contains the utility methods to interact with metadata, collections &
    dataObject.
    """

    def __init__(self, config='b2http.cfg'):
        """Constructor of the irodsInterface class."""
        cp = configparser.RawConfigParser()
        cp.read(config)
        host = cp.get('iRODS', 'host')
        user = cp.get('iRODS', 'user')
        passw = cp.get('iRODS', 'password')
        zone = cp.get('iRODS', 'zone')
        port = cp.get('iRODS', 'port')
        self.iconnection = {'host': host, 'password': passw, 'user': user,
                            'zone': zone, 'port': port}

    def connect(self):
        """Connect to irods and keep the session in self.sess."""
        self.sess = iRODSSession(**self.iconnection)

        logging.info('Connected to iRODS at %s' % self.iconnection['host'])
        return self.sess

    def getFile(self, filename):
        """Get an iterator to read the file."""
        if not os.path.basename(filename).startswith('GE.'):
            errmsg = 'Only files from GE can be downloaded.'
            response = {'Meta': {'elements': 0,
                                 'errors': 1,
                                 'status': 404},
                        'Response': {'data': None,
                                     'errors': errmsg}}
            raise Exception(json.dumps(response))

        result = None
        try:
            result = irodsFile(self.sess, filename)

        except irods.exception.NetworkException as e:
            result = None
            logging.error(str(e))

        return result

    def getMetadata(self, fullPath=None):
        """Return a list of tuples like (id, name, units, value)."""
        meta = self.sess.metadata.get(DataObject, fullPath)
        return meta

    def listFile(self, fullPath=None):
        """Retrieve basic information from a file."""
        try:
            do = self.sess.data_objects.get(fullPath)
            response = {'Meta': {'elements': 1,
                                 'errors': 0,
                                 'status': 200},
                        'Response': {'data': [do.name],
                                     'errors': None}}
        except Exception as e:
            response = {'Meta': {'elements': 0,
                                 'errors': 1,
                                 'status': 404},
                        'Response': {'data': None,
                                     'errors':
                                     'File not found or inaccessible'}}
            print(str(e))
        return json.dumps(response)

    def listDir(self, fullPath=None):
        """Return a list of iRODSDataObject."""
        try:
            coll = self.sess.collections.get(fullPath)
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)

        return coll
