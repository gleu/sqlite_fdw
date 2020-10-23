sqlite_fdw
==========

Foreign Data Wrapper for sqlite

This was my attempt to write a Foreign Data Wrapper. Unfortunately, it didn't go long. I may work on it again in the future, just to understand how to write a Foreign Data Wrapper.

If you're looking for a sqlite Foreign Data Wrapper, you shouldn't use this one as it lacks important features. Instead, have a look at https://github.com/pgspider/sqlite_fdw.git.

Compilation
-----------

To use this FDW, you first need to compile it. You'll need pg_config and the usual build toolset. Then just launch:

<pre>
make
make install
</pre>

And you're good to go.

Adding the extension
--------------------

Connect to your database, and execute this query:

<pre>
CREATE EXTENSION sqlite_fdw;
</pre>

Using it
--------

You first need to add a server. It will have an option, the sqlite file path. It must be readable by the postgres process.

<pre>
CREATE SERVER sqlite_server
  FOREIGN DATA WRAPPER sqlite_fdw
  OPTIONS (database '/var/lib/pgsql/test.db');
</pre>

Then you can create your foreign table. It will have one option, the table name on the sqlite database:

<pre>
CREATE FOREIGN TABLE local_t1(... columns ...)
  SERVER sqlite_server
  OPTIONS (table 'remote_table');
</pre>

Since 9.5, you can also import the tables of a specific schema in your sqlite
database, just like this :

<pre>
IMPORT FOREIGN SCHEMA public FROM SERVER sqlite_server INTO public;
</pre>

Now, to get the contents of the remote table, you just need to execute a SELECT query on it:

<pre>
SELECT * FROM local_t1;
</pre>
