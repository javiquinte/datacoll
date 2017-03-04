#!/usr/bin/env python

import irodsInterface
from irods.models import DataObject
from irods.models import Collection
import urllib2 as ul

years = ['1993']
nets = ['GE']
channels = ['BHZ.D']
stations = ['DSB']

dcUrl = 'http://localhost:8080/rda/datacoll'

root = '/geofonBak/archive'
i = irodsInterface.irodsInterface('irods.cfg')
i.connect()

def createColl(iRODSDir):
    print 'Creating collection for %s' % iRODSDir
    ul.Request(dcUrl + '/features')

for yc in i.listDir(root).subcollections:
    if yc.name not in years:
        continue

    for nc in yc.subcollections:
        if nc.name not in nets:
            continue

        for sc in nc.subcollections:
            if sc.name not in stations:
                continue

            for cc in sc.subcollections:
                if cc.name not in channels:
                    continue
                createColl(cc)
