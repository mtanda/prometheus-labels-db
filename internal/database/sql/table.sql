CREATE TABLE IF NOT EXISTS `metrics{{.MetricsPreSuffix}}` (
	-- use int for using auto increment
	metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
	namespace TEXT NOT NULL,
	metric_name TEXT NOT NULL,
	region TEXT NOT NULL,
	dimensions JSON NOT NULL,
	from_timestamp INT NOT NULL,
	to_timestamp INT NOT NULL,
	updated_at INT NOT NULL
);

CREATE TABLE IF NOT EXISTS `metrics{{.MetricsCurSuffix}}` (
	-- use int for using auto increment
	metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
	namespace TEXT NOT NULL,
	metric_name TEXT NOT NULL,
	region TEXT NOT NULL,
	dimensions JSON NOT NULL,
	from_timestamp INT NOT NULL,
	to_timestamp INT NOT NULL,
	updated_at INT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics ON `metrics{{.MetricsPreSuffix}}`(namespace, metric_name, region, dimensions);

CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics ON `metrics{{.MetricsCurSuffix}}`(namespace, metric_name, region, dimensions);

CREATE VIRTUAL TABLE IF NOT EXISTS `metrics_lifetime{{.MetricsLifetimePreSuffix}}` USING rtree_i32(metric_id, from_timestamp, to_timestamp);

CREATE VIRTUAL TABLE IF NOT EXISTS `metrics_lifetime{{.MetricsLifetimeCurSuffix}}` USING rtree_i32(metric_id, from_timestamp, to_timestamp);
