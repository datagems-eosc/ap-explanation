# Troubleshooting

## Common Errors

### ProvSQL Extension Not Found

**Error:** `ProvSQL extension is not installed on the PostgreSQL server`

**Cause:** Database missing the ProvSQL extension

**Solution:**
```bash
# Use the provided Docker image
cd dependencies/postgres-provsql
docker build -t postgres-provsql .
docker run -d -p 5432:5432 postgres-provsql
```

Or install ProvSQL manually on your PostgreSQL instance.

---

### Table Not Annotated

**Error:** `Table 'students' in schema 'public' is not annotated. Please annotate the table first.`

**Cause:** Attempting to compute provenance before annotating tables

**Solution:** Use the managed lifecycle, which handles annotation automatically:
```bash
POST /api/v1/aps/explanation
```

---

### Table or Schema Not Found

**Error:** `Table 'xyz' does not exist in schema 'public'`

**Cause:** Invalid table name or schema in AP

**Solution:** Verify table exists:
```sql
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public';
```

---

### Semiring Operation Not Supported

**Error:** `Aggregate results can only be explained with the formula semiring; requested: why`

**Cause:** Only `formula` explains aggregates (see [Limitations](architecture.md#aggregates))

**Solution:** Request all semirings (`POST /api/v1/aps/explanation`) or `formula`, or rewrite the query without aggregation.

---

### Query Refused

**Errors:**

- `Aggregates ProvSQL cannot explain: percentile_cont. Add a column with COUNT, SUM, ...`
- `EXCEPT and INTERSECT queries are not supported, rewrite INTERSECT as a JOIN with DISTINCT.`
- `The provenance query was stopped because it exceeded max_aggregated_rows (8,000,000)`
- `The provenance query was stopped because it exceeded the plain query's statement_timeout (180s)`

**Cause:** The query is refused before its provenance is computed. The last two come from the pre-check, which runs the query without provenance: its aggregates receive more rows than `PROVENANCE_MAX_AGGREGATED_ROWS`, or it takes longer than `PROVENANCE_PRECHECK_TIMEOUT` (see [Configuration](configuration.md#provenance-query-limits)).

**Solution:** Narrow the query. Raise `PROVENANCE_MAX_AGGREGATED_ROWS` only after [measuring](configuration.md#choosing-provenance_max_aggregated_rows) what the server holds.

---

### Query Stopped by a Limit

**Error:** `The provenance query was stopped because it exceeded statement_timeout (10min)` (or `provsql.max_memory`, `temp_file_limit`)

**Cause:** The query is too costly to explain within the limits set on it (see [Configuration](configuration.md#provenance-query-limits)). Its circuit stays in ProvSQL's store.

**Solution:** Narrow the query (fewer rows, a filter before the join or aggregate), or raise the limit, keeping the database's memory and disk in mind. Reclaim the store with a [circuit cleanup](usage.md#reclaiming-the-circuit-store).

---

## Connection Issues

### Cannot Connect to Database

The service connects with `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER` and `POSTGRES_PASSWORD`, to the database named by the AP's `RelationalDatabase` node:
```json
{ "labels": ["RelationalDatabase"], "properties": { "name": "mathe" } }
```

Verify:
- Host and port are correct
- Credentials are valid
- Database exists on either the primary PostgreSQL or Timescale server
- Network allows connection

### Database Not Found

**Error:** `Database 'dbname' not found on either PostgreSQL or Timescale instances`

**Cause:** The database doesn't exist on either the primary PostgreSQL server or the Timescale fallback server

**Solution:** 
1. Verify the `name` of the AP's `RelationalDatabase` node
2. Check if the database exists on the primary PostgreSQL server:
   ```bash
   psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -l
   ```
3. Check if the database exists on the Timescale server (if configured):
   ```bash
   psql -h $POSTGRES_TIMESCALE_HOST -p $POSTGRES_TIMESCALE_PORT -U $POSTGRES_USER -l
   ```
4. Create the database if it doesn't exist:
   ```sql
   CREATE DATABASE dbname;
   ```

### Missing Environment Variables

**Error:** `Missing required environment variables: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST`

**Cause:** Required environment variables are not set

**Solution:** Set the required environment variables:
```bash
export POSTGRES_USER=your_user
export POSTGRES_PASSWORD=your_password
export POSTGRES_HOST=your_host
export POSTGRES_PORT=5432  # optional, defaults to 5432
```

Or create a `.env` file with these variables if the service loads them automatically.
