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
import urllib
import json
from urllib.request import Request
from urllib.request import urlopen
from urllib.request import HTTPError

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from unittestTools import WITestRunner


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

    def test_coll_rule(self):
        """Rule based Collection."""
        with open('new-coll.json') as fin:
            data = fin.read()
            req = Request('%s/collections' % self.host, data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a collection
            try:
                u = urlopen(req)
                coll = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

        with open('new-memb.json') as fin:
            data = fin.read()
            req = Request('%s/collections/%d/members' %
                          (self.host, coll['collid']), data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a member
            try:
                u = urlopen(req)
                memb = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query the member to check it has been properly created
            req = Request('%s/collections/%d/members/%d' %
                                  (self.host, coll['collid'], memb['memberid']))
            try:
                u = urlopen(req)
                memb2 = json.loads(u.read())
                # Check that error code is 200
                self.assertEqual(u.getcode(), 200,
                                 'Error code 200 was expected!')
                # Check that the ids are the same
                self.assertEqual(memb['memberid'], memb2['memberid'], 'IDs differ!')
                # Compare owner with the original one
                msg = 'Location recorded differ with the original one!'
                self.assertEqual(json.loads(data)['location'],
                                 memb2['location'], msg)
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

        with open('new-coll-rule.json') as fin:
            data = fin.read()
            req = Request('%s/collections' % self.host, data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a collection
            try:
                u = urlopen(req)
                collrule = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query all the members belonging to the collection
            req = Request('%s/collections/%d/members/' %
                          (self.host, collrule['collid']))
            try:
                u = urlopen(req)
                memb2 = json.loads(u.read())
                # Check that error code is 200
                self.assertEqual(u.getcode(), 200,
                                 'Error code 200 was expected!')

                # FIXME What else should I check?
                # Probably that there is only one member!
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Delete the member
            req = Request('%s/collections/%d/members/%d' %
                          (self.host, coll['collid'], memb['memberid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            self.assertEqual(u.getcode(), 200,
                             'HTTP Error code 200 was expected!')

            # Delete the collection
            req = Request('%s/collections/%d' %
                                 (self.host, coll['collid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            self.assertEqual(u.getcode(), 200,
                             'HTTP Error code 200 was expected!')

            # Delete the collection with rule
            req = Request('%s/collections/%d' %
                          (self.host, collrule['collid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            self.assertEqual(u.getcode(), 200,
                             'HTTP Error code 200 was expected!')
            return

    def test_memb_create_query_delete(self):
        """Creation, query and deletion of a Member of a Collection."""
        with open('new-coll.json') as fin:
            data = fin.read()
            req = Request('%s/collections' % self.host, data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a collection
            try:
                u = urlopen(req)
                coll = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

        with open('new-memb.json') as fin:
            data = fin.read()
            req = Request('%s/collections/%d/members' %
                          (self.host, coll['collid']), data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a member
            try:
                u = urlopen(req)
                memb = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query the member to check it has been properly created
            req = Request('%s/collections/%d/members/%d' %
                          (self.host, coll['collid'], memb['memberid']))
            try:
                u = urlopen(req)
                memb2 = json.loads(u.read())
                # Check that error code is 200
                self.assertEqual(u.getcode(), 200,
                                 'Error code 200 was expected!')
                # Check that the ids are the same
                self.assertEqual(memb['memberid'], memb2['memberid'], 'IDs differ!')
                # Compare owner with the original one
                msg = 'Location recorded differ with the original one!'
                self.assertEqual(json.loads(data)['location'],
                                 memb2['location'], msg)
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query all the members belonging to the collection
            req = Request('%s/collections/%d/members/' %
                          (self.host, coll['collid']))
            try:
                u = urlopen(req)
                memb2 = json.loads(u.read())
                # Check that error code is 200
                self.assertEqual(u.getcode(), 200,
                                 'Error code 200 was expected!')

                # FIXME What else should I check?
                # Probably that there is only one member!
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # FIXME Check the syntax of "properties"
            # Query the collection capabilities
            # req = urllib2.Request('%s/collections/%d/capabilities' %
            # (self.host, coll['id']))
            # try:
            #     u = urllib2.urlopen(req)
            #     capab = json.loads(u.read())
            #     # Check that capabilities have at least the maxLength field
            #     self.assertEqual(capab['maxLength'], -1,
            # 'maxLength is supposed to be -1 for this test!')
            # except Exception as e:
            #     self.assertTrue(False, 'Error: %s' % e)

            # Delete the member
            req = Request('%s/collections/%d/members/%d' %
                          (self.host, coll['collid'], memb['memberid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            self.assertEqual(u.getcode(), 200,
                             'HTTP Error code 200 was expected!')

            # Delete the collection
            req = Request('%s/collections/%d' %
                          (self.host, coll['collid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            self.assertEqual(u.getcode(), 200,
                             'HTTP Error code 200 was expected!')
            return

    def test_coll_create_query_delete(self):
        """Creation, query and deletion of a Collection."""
        with open('new-coll.json') as fin:
            data = fin.read()
            req = Request('%s/collections' % self.host, data=data)
            req.add_header("Content-Type", 'application/json')
            # Create a collection
            try:
                u = urlopen(req)
                coll = json.loads(u.read())
                # Check that I received a 201 code
                self.assertEqual(u.getcode(), 201,
                                 'HTTP code 201 was expected!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query the collection to check it has been properly created
            req = Request('%s/collections/%d' %
                                  (self.host, coll['collid']))
            try:
                u = urlopen(req)
                coll2 = json.loads(u.read())
                # Check that I received a 200 code
                self.assertEqual(u.getcode(), 200,
                                 'HTTP code 200 was expected!')
                # Check that the ids are the same
                self.assertEqual(coll['collid'], coll2['collid'], 'IDs differ!')
                # Compare owner with the original one
                self.assertEqual(json.loads(data)['properties']['ownership'],
                                 coll2['properties']['ownership'],
                                 'Owner provided differ with original one!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Query the collection capabilities
            req = Request('%s/collections/%d/capabilities' %
                                 (self.host, coll['collid']))
            try:
                u = urlopen(req)
                capab = json.loads(u.read())
                # Check that I received a 200 code
                self.assertEqual(u.getcode(), 200,
                                 'HTTP code 200 was expected!')
                # Check that the capabilities have at least the maxLength field
                self.assertEqual(capab['maxLength'], -1,
                                 'maxLength supposed to be -1 for this test!')
            except Exception as e:
                self.assertTrue(False, 'Error: %s' % e)

            # Delete the collection
            req = Request('%s/collections/%d' %
                                 (self.host, coll['collid']))
            req.get_method = lambda: 'DELETE'
            u = urlopen(req)
            # Check that I received a 200 code
            self.assertEqual(u.getcode(), 200, 'HTTP code 200 was expected!')
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
