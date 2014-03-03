use ks_tests;

CREATE TABLE user ( name text PRIMARY KEY, id uuid, domain uuid );

CREATE INDEX ON user ( domain );
