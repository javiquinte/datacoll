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
import uuid
import hashlib
import mimetypes
from urllib.request import Request
from urllib.request import urlopen
from urllib.parse import urlparse
from urllib.error import HTTPError
from bson.json_util import loads
from bson.json_util import dumps

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
    def __init__(self, collid: str = None, memberid: str = None, location: str = None, checksum: str = None,
                 datatype: str = None, jsondesc: dict = None, futureloc: str = None, host: str = None):
        # DC System that this class should interact with
        self.host = host

        # If memberid is present the Member should be retrieved from the DC System
        if (memberid is not None) and (collid is not None):
            self.__getfromserver(collid, memberid)
            return

        # If there is a JSON description use it
        self.json = jsondesc if jsondesc is not None else dict()

        # If needed create mandatory entries in the JSON structure and copy from parameters
        if 'location' not in self.json:
            self.json['location'] = location if location is not None else ''
        if 'checksum' not in self.json:
            self.json['checksum'] = checksum if checksum is not None else ''
        if 'datatype' not in self.json:
            self.json['datatype'] = datatype if datatype is not None else ''

        u = urlparse(self.json['location'])
        if (not u.scheme) and (not u.netloc) and os.path.isfile(self.json['location']):
            do = DigitalObject(location, checksum, datatype)

            # Copy data from DO or from the expected location after publication
            self.json['location'] = do.uri if futureloc is None else futureloc

            if not len(self.json['datatype']):
                self.json['datatype'] = do.mimetype

            if not len(self.json['checksum']):
                self.json['checksum'] = do.checksum

            return

        # if ('pid' not in self.json) or (not len(self.json['pid'])):
        #     self.json['pid'] = str(uuid.uuid4())

    def __getfromserver(self, collid: str, memberid: str):
        # Check collid
        if (collid is None) or (not len(collid)):
            logging.error('collid must be valid and existing ID (%s)' % collid)
            raise Exception('collid must be valid and existing ID')

        # Check memberid
        if (memberid is None) or (not len(memberid)):
            logging.error('memberid must be valid and existing ID (%s)' % memberid)
            raise Exception('memberid must be valid and existing ID')

        # Query the member to check it has been properly created
        req = Request('%s/collections/%s/members/%s' % (self.host, collid, memberid))
        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        memb = loads(u.read())
        # Check that error code is 200
        if u.getcode() == 200:
            self.json = memb
            return

        raise Exception('Error retrieving member (%s).' % memberid)

    def save(self, collid: str):
        # If this is a new Member
        if '_id' not in self.json:
            req = Request('%s/collections/%s/members' %
                          (self.host, collid), data=dumps(self.json).encode())
            req.add_header("Content-Type", 'application/json')
            # Create a member
            try:
                u = urlopen(req)
                return loads(u.read())
            except Exception as e:
                return {'message': 'Error creating member'}
        else:
            # Update not yet implemented!
            logging.error('Update not yet implemented!')
            raise Exception('Update not yet implemented!')


class Collection(object):
    def __init__(self, collid: str = None, name: str = None, owner: str = None, jsondesc: dict = None,
                 directory: str = None, host: str = None):
        # Define the list of members in the Collection
        self.__members = list()

        # DC System that this class should interact with
        self.host = host

        # If collid is present the Collection should be retrieved from the DC System
        if collid is not None:
            self.__getfromserver(collid)
            return

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
        if 'capabilities' not in self.json:
            self.json['capabilities'] = dict()

        if ('pid' not in self.json) or (not len(self.json['pid'])):
            self.json['pid'] = str(uuid.uuid4())

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

        # Use the same host as the Collection
        member.host = self.host
        self.__members.append(member)

    def __getfromserver(self, collid: str):
        # Query the collection to check it has been properly created
        if (collid is None) or (not len(collid)):
            logging.error('collid must be valid and existing ID (%s)' % collid)
            raise Exception('collid must be valid and existing ID')

        # Request the collection
        req = Request('%s/collections/%s' % (self.host, str(collid)))
        # req.add_header("Authorization", "Bearer %s" % token)

        u = urlopen(req)
        coll = loads(u.read())
        # Check that error code is 200
        if u.getcode() == 200:
            self.json = coll

            # Retrieve also the members
            # Query the member to check it has been properly created
            req = Request('%s/collections/%s/members/' % (self.host, collid))
            u = urlopen(req)
            members = loads(u.read())
            # Check that error code is 200
            if u.getcode() == 200:
                for m in members:
                    self.addmember(Member(host=self.host, jsondesc=m))

            return

        raise Exception('Error retrieving collection %s.' % collid)

    def save(self):
        # If this is a new Collection
        if '_id' not in self.json:
            # First check that all members have a proper location
            for m in self:
                if 'location' in m.json:
                    u = urlparse(m.json['location'])
                    if not u.scheme or not u.netloc:
                        logging.error('Member %s does not have a proper location (%s)' % (m, m.json['location']))
                        raise Exception('Wrong location %s' % m.json['location'])
                elif isinstance(m, Member):
                    logging.error('Member without location %s' % m)
                    raise Exception('Member without location')

            # Create a collection
            req = Request('%s/collections' % self.host, data=dumps(self.json).encode())
            req.add_header("Content-Type", 'application/json')
            try:
                u = urlopen(req)
                coll = loads(u.read())
            except Exception as e:
                return {'message': 'Error creating collection'}

            for m in self:
                print(coll['_id'])
                m.save(coll['_id'])
        else:
            # Update not yet implemented!
            logging.error('Update not yet implemented!')
            raise Exception('Update not yet implemented!')


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
