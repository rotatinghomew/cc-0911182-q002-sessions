CREATE TABLE IF NOT EXISTS receipts(batch_id VARCHAR PRIMARY KEY, signature VARCHAR NOT NULL, inserted INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS source_rows(table_name VARCHAR, row_key VARCHAR, payload VARCHAR NOT NULL, batch_id VARCHAR NOT NULL, PRIMARY KEY(table_name,row_key));
        CREATE TABLE IF NOT EXISTS sites(site_id VARCHAR PRIMARY KEY, timezone VARCHAR, label VARCHAR);
        CREATE TABLE IF NOT EXISTS events(site_id VARCHAR, event_id VARCHAR, visitor_id VARCHAR, occurred_at VARCHAR, received_at VARCHAR, page VARCHAR, PRIMARY KEY(site_id,event_id));

CREATE TABLE IF NOT EXISTS site_days(site_id VARCHAR, day VARCHAR, hits INTEGER, visitors INTEGER, source_digest VARCHAR, PRIMARY KEY(site_id,day));