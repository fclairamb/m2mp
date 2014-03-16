use ks_test;

CREATE TABLE timeseries (
  id text,
  period int,
  date timeuuid,
  data text,
  type text,
  PRIMARY KEY ((id, period), date)
) WITH CLUSTERING ORDER BY (date DESC);
