#!/usr/bin/env python3

"""Tests to check the minimum Data Collection Service functionality is OK.

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

import sys
import os
import unittest
import json
from urllib.request import Request
from urllib.request import urlopen
from urllib.error import HTTPError
from bson.json_util import loads

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from unittestTools import WITestRunner


global token

with open(os.path.join(os.path.expanduser('~'), '.eidatoken')) as fin:
    token = fin.read().encode('utf-8')


def createcollection(baseurl, datafile):
    with open(datafile) as fin:
        data = fin.read().encode('utf-8')
        req = Request('%s/collections' % baseurl, data=data)
        req.add_header("Content-Type", 'application/json')
        req.add_header("Authorization", "Bearer %s" % token)
        # Create a collection
        u = urlopen(req)
        coll = json.loads(u.read())
        # Check that I received a 201 code

        if u.getcode() == 201:
            return str(coll['_id'])

    raise Exception('Collection not created.')


def createmember(baseurl, collid, datafile):
    with open(datafile) as fin:
        data = fin.read().encode()
        req = Request('%s/collections/%s/members' % (baseurl, collid), data=data)
        req.add_header("Content-Type", 'application/json')
        req.add_header("Authorization", "Bearer %s" % token)
        # Create a member
        u = urlopen(req)
        memb = json.loads(u.read())
        # Check that I received a 201 code

        if u.getcode() == 201:
            return str(memb['_id'])

    raise Exception('Member not created.')


def deletemember(baseurl, collid, memberid):
    # Delete a member of a collection
    req = Request('%s/collections/%s/members/%s' %
                  (baseurl, collid, memberid))
    req.get_method = lambda: 'DELETE'
    req.add_header("Authorization", "Bearer %s" % token)

    u = urlopen(req)
    # Check that error code is 200
    if u.getcode() == 200:
        return True

    raise Exception('Error deleting member.')


def deletecollection(baseurl, collid):
    # Delete a collection
    req = Request('%s/collections/%s' %
                  (baseurl, collid))
    req.get_method = lambda: 'DELETE'
    req.add_header("Authorization", "Bearer %s" % token)

    u = urlopen(req)
    # Check that error code is 200
    if u.getcode() == 200:
        return True

    raise Exception('Error deleting collection.')


def getmember(baseurl, collid, memberid=None):
    # Query the member to check it has been properly created
    if memberid is not None:
        req = Request('%s/collections/%s/members/%s' %
                      (baseurl, collid, memberid))
    else:
        req = Request('%s/collections/%s/members' %
                      (baseurl, collid))

    req.add_header("Authorization", "Bearer %s" % token)

    u = urlopen(req)
    memb = json.loads(u.read())
    # Check that error code is 200
    if u.getcode() == 200:
        return memb

    raise Exception('Error retrieving member(s).')


def getcollection(baseurl, collid=None):
    # Query the collection to check it has been properly created
    if collid is not None:
        req = Request('%s/collections/%s' %
                      (baseurl, str(collid)))
    else:
        req = Request('%s/collections' %
                      (baseurl))

    req.add_header("Authorization", "Bearer %s" % token)

    u = urlopen(req)
    coll = loads(u.read())
    # Check that error code is 200
    if u.getcode() == 200:
        return coll

    raise Exception('Error retrieving collection(s).')


def getcollcapabilities(baseurl, collid):
    # Query the collection capabilities
    req = Request('%s/collections/%s/capabilities' %
                  (baseurl, collid))

    req.add_header("Authorization", "Bearer %s" % token)

    u = urlopen(req)
    capab = json.loads(u.read())
    # Check that error code is 200
    if u.getcode() == 200:
        return capab

    raise Exception('Error retrieving capabilities.')


class DataCollTests(unittest.TestCase):
    """Test the functionality of the Data Collection Service."""

    @classmethod
    def setUp(cls):
        """Setting up test."""
        cls.host = host

    def test_features(self):
        """'features' method of the system."""
        req = Request('%s/features' % self.host)
        # Call the features method
        try:
            u = urlopen(req)
            feat = json.loads(u.read())
            # Check that the error code is 200
            self.assertEqual(u.getcode(), 200, 'Error code 200 was expected!')
            # Check that providesCollectionPids is among the keys
            self.assertTrue('providesCollectionPids' in feat.keys(),
                            'providesCollectionPids not in features fields!')
            # Check that ruleBasedGeneration is among the keys
            self.assertTrue('ruleBasedGeneration' in feat.keys(),
                            'ruleBasedGeneration not in features fields!')
        except Exception as e:
            self.assertTrue(False, 'Error: %s' % e)

        return

    # def test_coll_rule(self):
    #     """Rule based Collection."""
    #     collid = createcollection(self.host, 'new-coll.json')
    #     with open('new-memb.json') as fin:
    #         memb = json.load(fin)
    #     memberid = createmember(self.host, collid, 'new-memb.json')
    #     memb2 = getmember(self.host, collid, memberid)
    #
    #     # Check that the ids are the same
    #     self.assertEqual(memberid, memb2['memberid'], 'IDs differ!')
    #     # Compare owner with the original one
    #     msg = 'Location recorded differ with the original one!'
    #     self.assertEqual(memb['location'], memb2['location'], msg)
    #
    #     collruleid = createcollection(self.host, 'new-coll-rule.json')
    #     membersrule = getmember(self.host, collruleid)
    #
    #     # FIXME What else should I check?
    #     # Probably that there is only one member!
    #
    #     deletemember(self.host, collid, memberid)
    #     deletecollection(self.host, collid)
    #     deletecollection(self.host, collruleid)
    #     return

    def test_memb_create_query_delete(self):
        """Creation, query and deletion of a Member of a Collection."""

        collid = createcollection(self.host, 'new-coll.json')
        with open('new-memb.json') as fin:
            memb = json.load(fin)
        memberid = createmember(self.host, collid, 'new-memb.json')
        memb2 = getmember(self.host, collid, memberid)

        # Check that the ids are the same
        self.assertEqual(memberid, str(memb2['_id']), 'IDs differ!')

        # Get all members from the collection (only 1)
        members = getmember(self.host, collid)

        # TODO Check probably that there is only one member!

        deletemember(self.host, collid, memberid)
        deletecollection(self.host, collid)
        return

    def test_coll_create_query_delete(self):
        """Creation, query and deletion of a Collection."""
        with open('new-coll.json') as fin:
            coll = json.load(fin)
        collid = createcollection(self.host, 'new-coll.json')
        coll2 = getcollection(self.host, collid)

        # Check that the ids are the same
        self.assertEqual(collid, str(coll2['_id']), 'IDs differ!')
        # Compare owner with the original one
        self.assertEqual(coll['properties']['ownership'],
                         coll2['properties']['ownership'],
                         'Owner provided differ with original one!')

        # Query the collection capabilities
        capab = getcollcapabilities(self.host, collid)

        # Check that the capabilities have at least the maxLength field
        self.assertEqual(capab['maxLength'], -1,
                         'maxLength supposed to be -1 for this test!')

        deletecollection(self.host, collid)
        return

    def test_collections(self):
        """List of Collections."""
        with open('new-coll.json') as fin:
            coll = json.load(fin)
        collid1 = createcollection(self.host, 'new-coll.json')
        collid2 = createcollection(self.host, 'new-coll.json')
        coll2 = getcollection(self.host)

        # Check that the ids are the same
        collIds = {str(col['_id']) for col in coll2}
        collNames = {col['name'] for col in coll2}

        # Remove both IDs from both collections added
        collIds.remove(collid1)
        collIds.remove(collid2)
        # And the name
        collNames.remove(coll['name'])

        deletecollection(self.host, collid2)
        deletecollection(self.host, collid1)

        return

    def test_coll_missing(self):
        """Try to retrieve a non-existing Collection."""

        msg = 'Retrieving a non-existing collection should raise a HTTPError'
        with self.assertRaises(HTTPError):
            getcollection(self.host, 'non-existing-collection')
        return


global host

host = 'http://localhost:8080/rda/datacoll'


def usage():
    """Print a help message clarifying the usage of this script file."""
    pass


if __name__ == '__main__':

    # 0=Plain mode (good for printing); 1=Colourful mode
    mode = 1

    # The default host is localhost
    for ind, arg in enumerate(sys.argv):
        if arg in ('-p', '--plain'):
            del sys.argv[ind]
            mode = 0
        elif arg == '-u':
            host = sys.argv[ind + 1]
            del sys.argv[ind + 1]
            del sys.argv[ind]
        elif arg in ('-h', '--help'):
            usage()
            sys.exit(0)

    unittest.main(testRunner=WITestRunner(mode=mode))
