use ks_test;

CREATE TABLE timeseries (
  id text,
  date text,
  time timeuuid,
  data text,
  type text,
  PRIMARY KEY ((id, date), time)
) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE timeseries_index (
  id text,
  date text,
  type text,
  PRIMARY KEY ((id), date)
) WITH CLUSTERING ORDER BY (date DESC);
