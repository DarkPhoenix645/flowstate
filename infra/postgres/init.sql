-- Schema version 4. init.sql runs only on a fresh volume.
-- The process checks schema_version on connect.

CREATE TABLE schema_version (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO schema_version (version) VALUES (4);

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

-- One replay seed. Not a model-config version.
CREATE TABLE run_config (
    id integer PRIMARY KEY CHECK (id = 1),
    replay_seed integer NOT NULL
);

-- #26 and #86 write rows. run_id stays null until then.
CREATE TABLE benchmark (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text,
    engine text,
    corpus_rows bigint,
    seed integer,
    elapsed_seconds double precision,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One API unit of work. #88 and later jobs add rows of their kinds.
CREATE TABLE run (
    run_id text PRIMARY KEY,
    kind text NOT NULL CHECK (
        kind IN (
            'amplify', 'analytics', 'benchmark', 'replay', 'stream'
        )
    ),
    job text NOT NULL,
    params jsonb NOT NULL,
    config_snapshot jsonb NOT NULL,
    config_hash text,
    scenario_policy_version_id text,
    resample_config_version_id text,
    backend text NOT NULL,
    status text NOT NULL CHECK (
        status IN (
            'queued', 'running', 'succeeded', 'failed', 'cancelled'
        )
    ),
    external_id text,
    result_location text,
    grafana_url text,
    error text,
    log text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz
);
