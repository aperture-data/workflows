CREATE EXTENSION IF NOT EXISTS multicorn;

CREATE SERVER IF NOT EXISTS aperturedb FOREIGN DATA WRAPPER multicorn options (wrapper 'fdw.FDW');

CREATE SCHEMA IF NOT EXISTS system;
IMPORT FOREIGN SCHEMA system FROM SERVER aperturedb INTO system;

CREATE SCHEMA IF NOT EXISTS entity;
IMPORT FOREIGN SCHEMA entity FROM SERVER aperturedb INTO entity;

CREATE SCHEMA IF NOT EXISTS connection;
IMPORT FOREIGN SCHEMA connection FROM SERVER aperturedb INTO connection;

CREATE SCHEMA IF NOT EXISTS descriptor;
IMPORT FOREIGN SCHEMA descriptor FROM SERVER aperturedb INTO descriptor;

-- This allows users to access tables and functions within these schemata 
-- without needing to specify the schema name, unless there is ambiguity.
-- system, because it is first, is the default schema.
ALTER DATABASE aperturedb SET search_path TO system, entity, connection, descriptor;
ALTER ROLE postgres SET search_path TO system, entity, connection, descriptor;