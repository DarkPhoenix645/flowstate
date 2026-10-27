-- Schema version 1. init.sql runs only on a fresh volume.
-- The process checks schema_version on connect.

CREATE TABLE schema_version (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO schema_version (version) VALUES (1);

CREATE TABLE config_version (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL CHECK (
        kind IN ('scenario_policy', 'resample_config')
    ),
    content jsonb NOT NULL,
    sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    note text
);

CREATE OR REPLACE FUNCTION reject_config_version_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'config_version rows are immutable';
END;
$$;

CREATE TRIGGER config_version_immutable
    BEFORE UPDATE OR DELETE ON config_version
    FOR EACH ROW
    EXECUTE FUNCTION reject_config_version_change();

CREATE TABLE config_active (
    kind text PRIMARY KEY CHECK (
        kind IN ('scenario_policy', 'resample_config')
    ),
    version_id bigint NOT NULL REFERENCES config_version (id)
);
