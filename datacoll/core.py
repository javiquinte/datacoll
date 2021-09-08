#!/usr/bin/env python3

"""Client for a Data Collection Service

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

   :Copyright:
       2016-2017 Javier Quinteros, GEOFON, GFZ Potsdam <geofon@gfz-potsdam.de>
   :License:
       GPLv3
   :Platform:
       Linux

.. moduleauthor:: Javier Quinteros <javier@gfz-potsdam.de>, GEOFON, GFZ Potsdam
"""

import os
import logging
import json
import uuid
import hashlib
import mimetypes
from urllib.request import Request
from urllib.request import urlopen
from urllib.error import HTTPError
from bson.json_util import loads

# global token
#
# with open(os.path.join(os.path.expanduser('~'), '.eidatoken')) as fin:
#     token = fin.read().encode('utf-8')


class DigitalObject(object):
    """Representation of a digital object"""
    def __init__(self, uri: str, checksum: str = None, mimetype: str = None):
        # Possibly is a path
        if os.path.isfile(uri):
            uri = os.path.abspath(uri)
            if checksum is None:
                # Calculate checksum
                checksum = hashlib.md5(open(uri, 'rb').read()).hexdigest()

        self.uri = uri
        self.checksum = checksum

        if mimetype is None:
            # Guess mimetype
            self.mimetype, enc = mimetypes.guess_type(uri, strict=False)
        else:
            self.mimetype = mimetype


class Member(object):
    def __init__(self, location: str = None, checksum: str = None, datatype: str = None, jsondesc: dict = None,
                 futureloc: str = None):
        # Check that a datatype is defined
        if (datatype is None) and (jsondesc is not None) and ('datatype' in jsondesc):
            datatype = jsondesc['datatype']

        self.do = DigitalObject(location, checksum, datatype)

        self.json = jsondesc if jsondesc is not None else dict()
        # Copy data from DO or from the expected location after publication
        self.json['location'] = self.do.uri if futureloc is None else futureloc

        if ('datatype' not in self.json) or (not len(self.json['datatype'])):
            self.json['datatype'] = self.do.mimetype

        # Copy checksum from DO
        if ('checksum' not in self.json) or (not len(self.json['checksum'])):
            self.json['checksum'] = self.do.checksum

        # if ('pid' not in self.json) or (not len(self.json['pid'])):
        #     self.json['pid'] = str(uuid.uuid4())


class Collection(object):
    def __init__(self, name: str = None, owner: str = None, jsondesc: dict = None, directory: str = None):
        # Check that an owner is defined Priority is parameter, json, default owner
        if owner is not None:
            pass
        elif (jsondesc is not None) and ('properties' in jsondesc) and ('ownership' in jsondesc['properties']):
            owner = jsondesc['properties']['ownership']
        else:
            owner = ''

        # Check the name of the collection. Priority is parameter, json, directory name
        if name is not None:
            pass
        elif (jsondesc is not None) and ('name' in jsondesc):
            name = jsondesc['name']
        elif directory is not None:
            name = os.path.split(os.path.abspath(directory))[-1]
        else:
            raise Exception('Collection needs a name')

        self.json = jsondesc if jsondesc is not None else dict()
        self.json['name'] = name
        if 'properties' not in self.json:
            self.json['properties'] = dict()
        self.json['properties']['ownership'] = owner

        if ('pid' not in self.json) or (not len(self.json['pid'])):
            self.json['pid'] = str(uuid.uuid4())

        self.__members = list()

        if directory is not None:
            logging.info('Scanning directory %s' % directory)
            # TODO Add members after scanning

    def __iter__(self):
        for m in self.__members:
            yield m
        return

    def addmember(self, member: Member):
        if not isinstance(member, (Member, Collection)):
            raise Exception('A member can only be of type Member or Collection')
        self.__members.append(member)


# TODO Check this!
class DataCollectionClient(object):
    def __init__(self, host: str = None):
        self.host = host if host is not None else 'http://localhost:8080/rda/datacoll'

    def createcollection(self, datafile):
        with open(datafile) as fin:
            data = fin.read().encode('utf-8')
            req = Request('%s/collections' % self.host, data=data)
            req.add_header("Content-Type", 'application/json')
            # req.add_header("Authorization", "Bearer %s" % token)
            # Create a collection
            u = urlopen(req)
            coll = json.loads(u.read())
            # Check that I received a 201 code

            if u.getcode() == 201:
                return str(coll['_id'])

        raise Exception('Collection not created.')

    def createmember(self, collid, datafile):
        with open(datafile) as fin:
            data = fin.read().encode()
            req = Request('%s/collections/%s/members' % (self.host, collid), data=data)
            req.add_header("Content-Type", 'application/json')
            # req.add_header("Authorization", "Bearer %s" % token)
            # Create a member
            u = urlopen(req)
            memb = json.loads(u.read())
            # Check that I received a 201 code

            if u.getcode() == 201:
                return str(memb['_id'])

        raise Exception('Member not created.')

    def deletemember(self, collid, memberid):
        # Delete a member of a collection
        req = Request('%s/collections/%s/members/%s' %
                      (self.host, collid, memberid))
        req.get_method = lambda: 'DELETE'
        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        # Check that error code is 200
        if u.getcode() == 200:
            return True

        raise Exception('Error deleting member.')

    def deletecollection(self, collid):
        # Delete a collection
        req = Request('%s/collections/%s' %
                      (self.host, collid))
        req.get_method = lambda: 'DELETE'
        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        # Check that error code is 200
        if u.getcode() == 200:
            return True

        raise Exception('Error deleting collection.')

    def getmember(self, collid, memberid=None):
        # Query the member to check it has been properly created
        if memberid is not None:
            req = Request('%s/collections/%s/members/%s' %
                          (self.host, collid, memberid))
        else:
            req = Request('%s/collections/%s/members' %
                          (self.host, collid))

        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        memb = loads(u.read())
        # Check that error code is 200
        if u.getcode() == 200:
            return memb

        raise Exception('Error retrieving member(s).')

    def getcollection(self, collid=None):
        # Query the collection to check it has been properly created
        if collid is not None:
            req = Request('%s/collections/%s' %
                          (self.host, str(collid)))
        else:
            req = Request('%s/collections' %
                          (self.host))

        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        coll = loads(u.read())
        # Check that error code is 200
        if u.getcode() == 200:
            return coll

        raise Exception('Error retrieving collection(s).')

    def getcollcapabilities(self, collid):
        # Query the collection capabilities
        req = Request('%s/collections/%s/capabilities' %
                      (self.host, collid))

        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        capab = json.loads(u.read())
        # Check that error code is 200
        if u.getcode() == 200:
            return capab

        raise Exception('Error retrieving capabilities.')
