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
POST /api/v1/ap/annotate
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
- Database exists
- Network allows connection
