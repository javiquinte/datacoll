#!/usr/bin/env python

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
import urllib2
import json

here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, '..'))
from unittestTools import WITestRunner


class DataCollTests(unittest.TestCase):
    """Test the functionality of the Data Collection Service

    """
    @classmethod
    def setUp(cls):
        "Setting up test"
        cls.host = host

    def test_coll_create_query_delete(self):
        "creation, query and deletion of a Collection"

	with open('new-coll.json') as fin:
            data = fin.read()
        req = urllib2.Request('%s/collections' % self.host, data=data)
        req.add_header("Content-Type",'application/json')
        # Create a collection
        try:
            u = urllib2.urlopen(req)
            coll = json.loads(u.read())
            # Check that I received a 201 code
            self.assertEqual(u.getcode(), 201, 'HTTP code 201 was expected!')
        except Exception as e:
            self.assertTrue(False, 'Error: %s' % e)

        # Query the collection to check it has been properly created
        req = urllib2.Request('%s/collections/%d' % (self.host, coll['id']))
        try:
            u = urllib2.urlopen(req)
            coll2 = json.loads(u.read())
            # Check that the ids are the same
            self.assertEqual(coll['id'], coll2['id'], 'IDs differ!')
            # Compare owner with the original one
            self.assertEqual(json.loads(data)['properties']['ownership'],
                             coll2['properties']['ownership'],
                             'Owner recorded differ with the original one!')
        except Exception as e:
            self.assertTrue(False, 'Error: %s' % e)

        # Delete the collection
        req = urllib2.Request('%s/collections/%d' % (self.host, coll['id']))
        req.get_method = lambda: 'DELETE'
        u = urllib2.urlopen(req)
        self.assertEqual(u.getcode(), 200)
        return

global host

host = 'http://localhost:8080/rda/datacoll'

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
