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

##################################################################
#
# First all the imports
#
##################################################################


import os
import glob
import json
import imp
import fnmatch
from wsgicomm import WIContentError
from wsgicomm import WIClientError
from wsgicomm import WIURIError
from wsgicomm import WIError
from wsgicomm import send_plain_response
from wsgicomm import send_dynamicfile_response
from wsgicomm import send_error_response
import logging

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


class DCApp(object):
    def __init__(self):
        self.__action_table = {}
        self.__modules = {}

        # Take this directory as base and import what is on the modules subdir
        here = os.path.dirname(__file__)
        for f in glob.glob(os.path.join(here, "modules", "*.py")):
            self.__load_module(f)

    def __load_module(self, path):
        modname = os.path.splitext(os.path.basename(path))[0].replace('.', '_')
        logging.debug('Importing %s from %s' % (modname, path))

        if modname in self.__modules:
            logging.error("'%s' is already loaded!" % modname)

        try:
            mod = imp.load_source('__dc_' + modname, path)
            logging.info("Module '%s' imported!" % modname)
        except:
            logging.error("Error loading '%s'" % modname)
            return

        self.__modules[modname] = mod.DC_Module(self)

    def registerAction(self, name, func):
        # FIXME Check that there are no collissions with existing keys
        self.__action_table[name] = func

    def getAction(self, name):
        # Important modification: If there is no perfect match a partial match
        # will be tested
        result = list()

        for k in self.__action_table.keys():
            # Compare entries in both lists (k:registered and name:given)
            for idx in range(0, len(k)):
                # print name[idx] if len(name) > idx else '', k[idx], \
                #    fnmatch.fnmatch(name[idx] if len(name) > idx else '', k[idx])
                if not fnmatch.fnmatch(name[idx] if len(name) > idx else '',
                                       k[idx]):
                    break
            else:
                # Register in a list the number of matches and the entry
                logging.debug('%s matches the method called: %s' % (k, name))
                result.append((len(k), k))

        try:
            # Search for the one with the max number of matches. Get the first
            # one in case that there are more than one (shouldn't be)
            k = [x[1] for x in result if x[0] == max(result)[0]][0]
            logging.debug('%s is the selected method' % repr(k))
            return self.__action_table[k]
        except:
            pass
        raise Exception('Action not found!')


#####################################################################
#
# Initialization of logging level and the main application variables
#
#####################################################################

config = configparser.RawConfigParser()
here = os.path.dirname(__file__)
config.read(os.path.join(here, 'datacoll.cfg'))
verbo = config.get('Service', 'verbosity')
# 'WARNING' is the default value
verboNum = getattr(logging, verbo.upper(), 30)
logging.basicConfig(level=verboNum)
logging.info('Verbosity configured with %s' %
              logging.getLogger().getEffectiveLevel())

dc = DCApp()

def application(environ, start_response):
    """Main WSGI handler that processes client requests and calls
    the proper functions.

    """

    fname = environ['PATH_INFO']
    splitFname = fname.strip('/').split('/')
    logging.debug('splitFname: %s' % splitFname)

    item = dc.getAction(splitFname)
    logging.debug('item: %s' % str(item))

    # Among others, this will filter wrong function names,
    # but also the favicon.ico request, for instance.
    if item is None:
        return send_error_response('400 Bad Request',
                                   'Method name not recognized!',
                                   start_response)

    if len(environ['QUERY_STRING']) > 1000:
        return send_error_response("414 Request URI too large",
                                   "maximum URI length is 1000 characters",
                                   start_response)

    action = item
    # (action, multipar) = item
    # parameters = {}
    try:
        iterObj = action(environ)

        status = '200 OK'
        if isinstance(iterObj, basestring):
            return send_plain_response(status, iterObj, start_response)
        else:
            return send_dynamicfile_response(status, iterObj, start_response)

    except WIError as w:
        return send_error_response(w.status, w.body, start_response)

    raise Exception('This point should have never been reached!')


def main():
    pass


if __name__ == "__main__":
    main()
