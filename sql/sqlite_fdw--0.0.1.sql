/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper  sqlite
 *
 * Copyright (c) 2013-2016, Guillaume Lelarge
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Guillaume Lelarge <guillaume@lelarge.info>
 *
 * IDENTIFICATION
 *                sqlite_fdw/=sql/sqlite_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION sqlite_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION sqlite_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER sqlite_fdw
  HANDLER sqlite_fdw_handler
  VALIDATOR sqlite_fdw_validator;
