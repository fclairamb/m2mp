use ks_test;

CREATE TABLE registrynode (
  path text,
  status int,
  values map<text, text>,
  PRIMARY KEY (path)
);

CREATE INDEX ON registrynode (status);

CREATE TABLE registrynodechildren (
  path text,
  name text,
  PRIMARY KEY (path, name)
);

CREATE TABLE registrynodedata (
  path text,
  block int,
  data blob,
  PRIMARY KEY (path, block)
);
