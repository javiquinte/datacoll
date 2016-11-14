.. Data Collections WS documentation master file, created by
   sphinx-quickstart on Mon Nov 14 15:17:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Data Collections WS's documentation!
===============================================

Contents:

.. toctree::
   :maxdepth: 2

Installation
============

Requirements
------------

* Python 2.7

* mod_wsgi (if using Apache). Also Python libraries for libxslt and libxml.

.. _download:

Download
--------

Download the tar file / source from the GEOFON web page at
http://geofon.gfz-potsdam.de/software.

.. note ::
   Nightly builds will be available soon from Github.

Untar into a suitable directory visible to the web server,
such as `/var/www/rda/datacoll/` ::

    $ cd /var/www/rda/datacoll
    $ tar xvzf /path/to/tarfile.tgz

This location will depend on the location of the root (in the file system)
for your web server.

.. _oper_installation-on-apache:

Installation on Apache
----------------------

To deploy the EIDA Routing Service on an Apache2 web server using `mod_wsgi`:

#. Extract the package in the desired directory.
   In these instructions we assume this directory is `/var/www/rda/datacoll/`.

   If you downloaded the package from the GEOFON website, unpack the files into
   the chosen directory. (See Download_ above.)

   * If you want to get the package from Github, use the following commands: ::

    $ cd /var/www/rda/datacoll
    $ git clone https://github.com/toBeCompleted

#. Enable `mod_wsgi`. For openSUSE, add 'wsgi' to the list of modules in the
   APACHE_MODULES variable in `/etc/sysconfig/apache2` ::

    APACHE_MODULES+=" python wsgi"

   and restart Apache. You should now see the following line in your
   configuration (in `/etc/apache2/sysconfig.d/loadmodule.conf` for
   **openSUSE**) ::

    LoadModule wsgi_module   /usr/lib64/apache2/mod_wsgi.so

   You can also look at the output from ``a2enmod -l`` - you should see wsgi
   listed.

   For **Ubuntu/Mint**, you can enable the module with the command ::

    $ sudo a2enmod wsgi

   and you can restart apache with::

    $ sudo service apache2 stop
    $ sudo service apache2 start

   If the module was added successfully you should see the following two links
   in ``/etc/apache2/mods-enable`` ::

    wsgi.conf -> ../mods-available/wsgi.conf
    wsgi.load -> ../mods-available/wsgi.load

   For any distribution there may be a message like this in Apache's `error_log`
   file, showing that `mod_wsgi` was loaded ::

    [Tue Jul 16 14:24:32 2013] [notice] Apache/2.2.17 (Linux/SUSE)
    PHP/5.3.5 mod_python/3.3.1 Python/2.7 mod_wsgi/3.3 configured
    -- resuming normal operations

#. Add the following lines to a new file, `conf.d/datacoll.conf`, or in
   `default-server.conf`, or in the configuration for your virtual host. ::

    WSGIScriptAlias /rda/datacoll /var/www/rda/datacoll/datacoll.wsgi
    <Directory /var/www/rda/datacoll/>
      Order allow,deny
      Allow from all
    </Directory>

   Change `/var/www/rda/datacoll` to suit your own web server's needs.

#. Change into the root directory of your installation and copy
   `datacoll.cfg.sample` to `datacoll.cfg`, or make a symbolic link ::

    $ cd /var/www/rda/datacoll
    $ cp datacoll.cfg.sample datacoll.cfg

#. Edit `datacoll.wsgi` and check that the paths there reflect the ones selected
   for your installation.

#. Edit `datacoll.cfg` and be sure to configure everything correctly. This is
   discussed under "`Configuration Options`_" below.

#. Start/restart the web server e.g. as root. In **OpenSUSE** ::

    $ /etc/init.d/apache2 configtest
    $ /etc/init.d/apache2 restart

   or in **Ubuntu/Mint** ::

    $ sudo service apache2 reload
    $ sudo service apache2 stop
    $ sudo service apache2 start

#. Restart the web server to apply all the changes, e.g. as root. In
   **OpenSUSE**::

    $ /etc/init.d/apache2 configtest
    $ /etc/init.d/apache2 restart

   or in **Ubuntu/Mint** ::

    $ sudo service apache2 reload
    $ sudo service apache2 stop
    $ sudo service apache2 start


.. _configuration-options-extra:

Configuration options
^^^^^^^^^^^^^^^^^^^^^

The configuration file contains two sections up to this moment.

MongoDB
"""""""

In the MongoDB section a server must be defined, from which the
actual data collections can be retrieved and also stored.

.. code-block:: ini

    [MongoDB]
    server = server.domainname
    port = 27017

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

Installation problems
^^^^^^^^^^^^^^^^^^^^^

Always check your web server log files (e.g. for Apache: ``access_log`` and
``error_log``) for clues.

If you visit http://localhost/rda/datacoll/version on your machine
you should see the version information of the deployed service ::

    0.1a1

If this information cannot be retrieved, the installation was not successful.
If this **do** show up, check that the information there looks correct.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
