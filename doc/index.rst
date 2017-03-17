.. Data Collections WS documentation master file, created by
   sphinx-quickstart on Mon Nov 14 15:17:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Data Collections WS's documentation!
===============================================

Contents:

.. toctree::
   :maxdepth: 2

Summary
=======

Building collections within diverse domains and then sharing or expanding them
across disciplines should enable common tools for end-users and e-infrastructure
providers. Individual disciplinary communities can directly benefit if such
tools are made widely available, and cross-community data sharing can benefit
from increased unification between collection models and implementations. PID
providers may benefit from marketing additional services on collections.

A living API prototype has been made available by the RDA working group at
http://rdacollectionswg.github.io/apidocs/# .

More information about the *Research Data Collections* working group can be
found at https://www.rd-alliance.org/groups/pid-collections-wg.html .

Installation
============

Requirements
------------

* Python 2.7
* CherryPy for Python 2.X

.. _download:

Download
--------

Download the tar file / source from the GEOFON web page at
http://geofon.gfz-potsdam.de/software.

.. note ::
   Nightly builds will be available soon from Github.

Untar into a suitable directory such as `/var/www/rda/datacoll/` ::

    $ cd /var/www/rda/datacoll
    $ tar xvzf /path/to/tarfile.tgz

This location will depend on the location of the root (in the file system)
for your web server.

.. _installation:

Installation
------------

This version of the Data Collection System runs as a standalone application.
There is no need to deploy it on top of Apache.

You can start the application by running: ::

    $ python datacoll.py
    ENGINE Listening for SIGHUP.
    ENGINE Listening for SIGTERM.
    ENGINE Listening for SIGUSR1.
    ENGINE Bus STARTING
    CherryPy Checker:
    The Application mounted at '/rda/datacoll' has an empty config.
    ENGINE Started monitor thread 'Autoreloader'.
    ENGINE Started monitor thread '_TimeoutMonitor'.
    ENGINE Serving on http://127.0.0.1:8080
    ENGINE Bus STARTED

The system will listen to the port 8080.

.. _configuration-options-extra:

Configuration options
^^^^^^^^^^^^^^^^^^^^^

The configuration file contains two sections up to this moment.

Service
"""""""

`verbosity` controls the amount of output send to the logging system depending
of the importance of the messages. The levels are: 1) Error, 2) Warning, 3)
Info and 4) Debug.

.. _service_configuration:

.. code-block:: ini

    [Service]
    # Possible values are:
    # CRITICAL, ERROR, WARNING, INFO, DEBUG
    verbosity = INFO

MySQL
"""""

In the mysql section a server must be defined, from which the actual data
collections can be retrieved and also stored.

.. code-block:: ini

    [mysql]
    host = localhost
    user = dcusername
    password = dcpassword
    db = datacoll
    limit = 500

Installation problems
^^^^^^^^^^^^^^^^^^^^^

Always check the log output for clues.

If you visit http://localhost/rda/datacoll/version on your machine
you should see the version information of the deployed service ::

    0.1b1

If this information cannot be retrieved, the installation was not successful.
If this **does** show up, check that the information there looks correct.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
