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
import cgi
import json
import imp
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
        self.__action_table[name] = func

    def getAction(self, name):
        logging.debug('%s' % self.__action_table)
        return self.__action_table.get(name)

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
    logging.debug('fname: %s' % fname)

    item = dc.getAction(fname)
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

    # The keep_blank_values=1 is needed to recognize the download key despite
    # that it has no value associated (e.g. api/registered/fullpath?download)
    form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                            keep_blank_values=1)

    logging.debug(form.keys())

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
