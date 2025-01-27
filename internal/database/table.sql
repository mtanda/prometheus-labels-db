DROP TABLE IF EXISTS metrics;

CREATE TABLE metrics (
	-- use int for using auto increment
	metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
	namespace TEXT NOT NULL,
	name TEXT NOT NULL,
	region TEXT NOT NULL,
	dimensions JSON NOT NULL,
	from_timestamp INT NOT NULL,
	to_timestamp INT NOT NULL,
	updated_at INT NOT NULL
);

CREATE UNIQUE INDEX idx_metrics ON metrics(namespace, name, region, dimensions);

DROP TABLE IF EXISTS metrics_lifetime;

CREATE VIRTUAL TABLE metrics_lifetime USING rtree_i32(metric_id, from_timestamp, to_timestamp);
