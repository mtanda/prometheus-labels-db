CREATE TABLE IF NOT EXISTS metrics (
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics ON metrics(namespace, name, region, dimensions);

CREATE VIRTUAL TABLE IF NOT EXISTS metrics_lifetime USING rtree_i32(metric_id, from_timestamp, to_timestamp);
