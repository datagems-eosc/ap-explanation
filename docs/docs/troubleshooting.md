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

**Error:** `Table 'students' in schema 'public' is not annotated with semiring 'formula'`

**Cause:** Attempting to explain before annotating tables

**Solution:** Annotate the table first:
```bash
POST /api/v1/aps/annotate
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

**Error:** `Semiring 'why' does not support aggregate queries`

**Cause:** Using a semiring without aggregate function support on aggregation queries

**Solution:** Use the `formula` semiring which supports aggregates, or rewrite query without aggregation.

---

## Connection Issues

### Cannot Connect to Database

Check connection string format in AP:
```json
{
  "properties": {
    "contentUrl": "postgresql://user:password@host:5432/dbname"
  }
}
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
1. Verify the database name in the AP's `contentUrl` is correct
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
