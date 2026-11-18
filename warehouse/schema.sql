CREATE TABLE IF NOT EXISTS receipts(batch_id VARCHAR PRIMARY KEY, signature VARCHAR NOT NULL, inserted INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS source_rows(table_name VARCHAR, row_key VARCHAR, payload VARCHAR NOT NULL, batch_id VARCHAR NOT NULL, PRIMARY KEY(table_name,row_key));
        CREATE TABLE IF NOT EXISTS sites(site_id VARCHAR PRIMARY KEY, timezone VARCHAR, label VARCHAR);
        CREATE TABLE IF NOT EXISTS events(site_id VARCHAR, event_id VARCHAR, visitor_id VARCHAR, occurred_at VARCHAR, received_at VARCHAR, page VARCHAR, PRIMARY KEY(site_id,event_id));

-- Daily mart.  hits/visitors follow the local date of each event (visits);
-- sessions follows the local date on which the session started.  state says
-- whether the day is still inside the backfill window, and source_digest ties
-- the row to the exact accepted events it was computed from.
CREATE TABLE IF NOT EXISTS site_days(
    site_id VARCHAR, day VARCHAR, hits INTEGER, visitors INTEGER,
    source_digest VARCHAR, sessions INTEGER, state VARCHAR,
    PRIMARY KEY(site_id,day));

-- One row per derived visit session.  session_id is derived from the session's
-- earliest event so recomputation is deterministic and a surviving id remains
-- a valid reference when sessions merge.
CREATE TABLE IF NOT EXISTS sessions(
    site_id VARCHAR, session_id VARCHAR, visitor_id VARCHAR,
    start_at VARCHAR, end_at VARCHAR, start_ts BIGINT, end_ts BIGINT,
    hits INTEGER, entry_page VARCHAR, start_day VARCHAR,
    PRIMARY KEY(site_id,session_id));

-- Current membership of every accepted event.  A reference to a session that no
-- longer exists after a merge resolves through the event it was built from.
CREATE TABLE IF NOT EXISTS session_events(
    site_id VARCHAR, event_id VARCHAR, session_id VARCHAR,
    PRIMARY KEY(site_id,event_id));

-- Explicit destination of every raw event, including those that cannot enter
-- the fact window.
CREATE TABLE IF NOT EXISTS event_dispositions(
    site_id VARCHAR, event_id VARCHAR, status VARCHAR, reason VARCHAR,
    occurred_at VARCHAR, received_at VARCHAR, age_seconds BIGINT,
    window_seconds BIGINT, session_id VARCHAR,
    PRIMARY KEY(site_id,event_id));

-- Audit trail of published values that changed on recomputation.
CREATE TABLE IF NOT EXISTS fact_restatements(
    site_id VARCHAR, day VARCHAR, cause VARCHAR, observed_at VARCHAR,
    before_values VARCHAR, after_values VARCHAR);
