\set ON_ERROR_STOP on

\set db_name  metastore
\set hive_user hive

SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'hive_user') AS role_exists \gset
\if :role_exists
  ALTER ROLE :"hive_user" WITH LOGIN PASSWORD :'hive_pass';
\else
  CREATE ROLE :"hive_user" LOGIN PASSWORD :'hive_pass';
\endif

SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name') AS db_exists \gset
\if :db_exists
\else
  CREATE DATABASE :"db_name" OWNER :"hive_user";
\endif

ALTER DATABASE :"db_name" OWNER TO :"hive_user";
REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"db_name" TO :"hive_user";
