#!/bin/sh
# Find PROVENANCE_MAX_AGGREGATED_ROWS for a PostgreSQL + ProvSQL server: the largest aggregate
# ProvSQL still stops cleanly there, halved.
#
# This crashes the server it runs on: use a copy of production (same RAM, same settings),
# never production itself.
#
# Usage: sh calibrate_max_aggregated_rows.sh "host=... user=... password=..." [budget, default 1GB]
#
# Each step sums n x n rows, the shape of a reading x elevation point join, under
# provsql.max_memory, in a scratch database, and reads its formula: its first rows with
# sr_formula_rows (ProvSQL 1.14), or all of it with sr_formula (1.13). It stops at the first n
# where the server dies, or the step hangs past 15 minutes, instead of failing the query.
conn=$1; budget=${2:-1GB}
db=calibrate_aggregated_rows
safe=0
for n in 1000 1500 2000 2500 3000 3500 4000 4500 5000 6000 7000 8000 9000 10000; do
  until psql "$conn dbname=postgres" -qAtc "SELECT 1" >/dev/null 2>&1; do sleep 2; done  # back up after a crash
  psql "$conn dbname=postgres" -qc "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db" >/dev/null
  out=$(timeout 900 psql "$conn dbname=$db" -X -q 2>&1 <<SQL
CREATE EXTENSION IF NOT EXISTS provsql CASCADE;
SET search_path TO public, provsql;
SET provsql.active = 1;
CREATE TABLE a AS SELECT i AS id, i::float8 AS v FROM generate_series(1, $n) i;
SELECT add_provenance('a');
CREATE TABLE b AS SELECT i AS id FROM generate_series(1, $n) i;
SELECT add_provenance('b');
SELECT create_provenance_mapping('a_map', 'a', 'id');
SET provsql.max_memory = '$budget';
SET statement_timeout = '600s';
SELECT to_regproc('provsql.sr_formula_rows') IS NOT NULL AS has_rows \gset
\\if :has_rows
SELECT sr_formula_rows(x.s, 'a_map') FROM (SELECT SUM(a.v) AS s FROM a, b) AS x LIMIT plain(4);
\\else
SELECT length(sr_formula(x.s, 'a_map')) FROM (SELECT SUM(a.v) AS s FROM a, b) AS x;
\\endif
SQL
)
  hung=$?
  rows=$((n * n))
  if [ "$hung" = 124 ]; then
    echo "$rows rows: still running after 15 minutes, which a cancel does not stop: restart the server, then drop $db"
    echo "Largest aggregate stopped cleanly: $safe rows. Suggested PROVENANCE_MAX_AGGREGATED_ROWS=$((safe / 2))"
    exit 1
  fi
  if echo "$out" | grep -q "closed the connection\|terminated abnormally"; then
    echo "$rows rows: server crashed"
    break
  fi
  echo "$rows rows: stopped or answered"
  safe=$rows
done
until psql "$conn dbname=postgres" -qAtc "SELECT 1" >/dev/null 2>&1; do sleep 2; done
psql "$conn dbname=postgres" -qc "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
echo "Largest aggregate stopped cleanly: $safe rows. Suggested PROVENANCE_MAX_AGGREGATED_ROWS=$((safe / 2))"
