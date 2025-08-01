-- Prevent user from accidentally altering our schemata.

-- Revoke default privileges for everyone
REVOKE ALL ON SCHEMA system FROM PUBLIC;
REVOKE ALL ON SCHEMA entity FROM PUBLIC;
REVOKE ALL ON SCHEMA connection FROM PUBLIC;
REVOKE ALL ON SCHEMA descriptor FROM PUBLIC;

-- Grant read access to your user
GRANT USAGE ON SCHEMA system TO aperturedb;
GRANT USAGE ON SCHEMA entity TO aperturedb;
GRANT USAGE ON SCHEMA connection TO aperturedb;
GRANT USAGE ON SCHEMA descriptor TO aperturedb;

GRANT SELECT ON ALL TABLES IN SCHEMA system TO aperturedb;
GRANT SELECT ON ALL TABLES IN SCHEMA entity TO aperturedb;
GRANT SELECT ON ALL TABLES IN SCHEMA connection TO aperturedb;
GRANT SELECT ON ALL TABLES IN SCHEMA descriptor TO aperturedb;

GRANT USAGE ON FOREIGN SERVER aperturedb TO aperturedb;