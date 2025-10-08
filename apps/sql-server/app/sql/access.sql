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

-- Lock down public
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM aperturedb;

-- Drop extensions we don't need
DO $$
DECLARE
    ext record;
BEGIN
    FOR ext IN
        SELECT extname FROM pg_extension
        WHERE extname NOT IN ('plpgsql', 'multicorn')
    LOOP
        RAISE NOTICE 'Dropping unexpected extension: %', ext.extname;
        EXECUTE format('DROP EXTENSION IF EXISTS %I CASCADE;', ext.extname);
    END LOOP;
END;
$$;

-- Drop languages we don't need
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT lanname FROM pg_language
        WHERE lanname NOT IN ('internal', 'c', 'sql', 'plpgsql')
    LOOP
        RAISE NOTICE 'Dropping unapproved language: %', r.lanname;
        EXECUTE format('DROP LANGUAGE IF EXISTS %I CASCADE;', r.lanname);
    END LOOP;
END;
$$;