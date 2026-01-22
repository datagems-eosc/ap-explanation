# Semiring Mapping Strategies

The provenance system uses a **strategy pattern** for different mapping approaches. Each semiring has its own `ProvenanceMapping` instance that determines how database rows are mapped to provenance values.

## Architecture

```python
# Abstract base class
class ProvenanceMapping(ABC):
    def get_mapping_target_sql(self, table_name: str) -> str: ...
    def can_decode(self) -> bool: ...

# Concrete implementations
class CtidReferenceMapping(ProvenanceMapping): ...
class ConstantMapping(ProvenanceMapping): ...
class CustomExpressionMapping(ProvenanceMapping): ...
```

## Mapping Strategies

### 1. **CtidReferenceMapping**
Uses PostgreSQL's internal row identifier (ctid) to create unique, decodable references.

**Format:** `'{table_name}@p{page}r{row}'`

**Example:** `'assessment@p108r52'` refers to row at ctid (108,52) in table 'assessment'

**Usage:**
```python
from provenance_demo.repository.provenance_mapping import CtidReferenceMapping
from provenance_demo.types.semiring import DbSemiring

# Or use the helper function
from provenance_demo.types.semiring import create_why_semiring

why_semiring = create_why_semiring()
# Equivalent to:
why_semiring = DbSemiring(
    name="why",
    retrieval_function="whyprov_now",
    aggregate_function="aggregation_formula",
    mapping_table="why_mapping",
    mapping=CtidReferenceMapping()
)
```

**Features:**
- `can_decode()` returns `True`
- Has `decode_row_reference(value)` method to extract table and ctid
- Full traceability back to source rows

---

### 2. **ConstantMapping**
Uses a constant value for all rows. Perfect for counting semirings.

**Format:** A constant value like `'1'`

**Usage:**
```python
from provenance_demo.repository.provenance_mapping import ConstantMapping
from provenance_demo.types.semiring import DbSemiring

# Or use the helper function
from provenance_demo.types.semiring import create_counting_semiring

counting_semiring = create_counting_semiring()
# Equivalent to:
counting_semiring = DbSemiring(
    name="counting",
    retrieval_function="counting",
    aggregate_function="counting_plus",
    mapping_table="counting_mapping",
    mapping=ConstantMapping("1")  # Every row contributes 1
)
```

**Features:**
- `can_decode()` returns `False`
- No decoding back to specific rows
- Minimal storage and computation overhead

**SQL Generation:**
```python
mapping = ConstantMapping("1")
mapping.get_mapping_target_sql('assessment')  # Returns: "'1'"
```

---

### 3. **CustomExpressionMapping**
Uses a custom SQL expression to generate identifiers. Great for human-readable provenance.

**Format:** Any SQL expression, e.g., `"'a'||id"` or `"table_name||':'||primary_key"`

**Example:** `'a123'` for assessment with id=123

**Usage:**
```python
from provenance_demo.repository.provenance_mapping import CustomExpressionMapping
from provenance_demo.types.semiring import DbSemiring

# Or use the helper function
from provenance_demo.types.semiring import create_formula_semiring

formula_semiring = create_formula_semiring("'a'||id")
# Equivalent to:
formula_semiring = DbSemiring(
    name="formula",
    retrieval_function="formula",
    aggregate_function="formula_plus",
    mapping_table="formula_mapping",
    mapping=CustomExpressionMapping("'a'||id", decodable=False)
)
```

**Features:**
- `can_decode()` returns value of `decodable` parameter
- Set `decodable=True` if expression uniquely identifies rows
- Human-readable provenance values

**SQL Generation:**
```python
mapping = CustomExpressionMapping("'a'||id")
mapping.get_mapping_target_sql('assessment')  # Returns: "'a'||id"
```

---

## Comparison Table

| Strategy | Use Case | Example Value | can_decode() | Requires ID Column |
|----------|----------|---------------|--------------|-------------------|
| `CtidReferenceMapping` | Why-provenance, full traceability | `'assessment@p108r52'` | True | No |
| `ConstantMapping` | Counting, simple aggregation | `'1'` | False | No |
| `CustomExpressionMapping` | Formula semiring, readable IDs | `'a123'` | Configurable | Usually |

---

## Implementation in Repositories

### SemiringRepository

When enabling provenance for a table, the repository uses the mapping's `get_mapping_target_sql()`:

```python
# In SemiringRepository.enable_for()
await self._conn.execute(
    "SELECT create_provenance_mapping(%s, %s, %s)",
    (prov_table, table_name,
     self._semiring.mapping.get_mapping_target_sql(table_name))
)
```

### ProvenanceRepository

When decoding provenance data, the repository uses the appropriate mapping:

```python
# In ProvenanceRepository.retrieve_provenance_data()
ctid_mapping = CtidReferenceMapping()
for ref in references:
    table_name, ctid = ctid_mapping.decode_row_reference(ref)
    # Query database for the row...
```

---

## Creating Custom Mapping Strategies

You can create your own mapping strategy by inheriting from `ProvenanceMapping`:

```python
from provenance_demo.repository.provenance_mapping import ProvenanceMapping

class MyCustomMapping(ProvenanceMapping):
    def __init__(self, some_config: str):
        self.config = some_config
    
    def get_mapping_target_sql(self, table_name: str) -> str:
        # Return SQL that generates your custom format
        return f"'{self.config}'||{table_name}_id"
    
    def can_decode(self) -> bool:
        return True  # or False
    
    def decode_value(self, value: str) -> dict:
        # Optional: implement custom decoding logic
        pass
```

Then use it in your semiring:

```python
my_semiring = DbSemiring(
    name="custom",
    retrieval_function="my_function",
    mapping_table="my_mapping",
    mapping=MyCustomMapping("prefix_")
)
```

---

## Helper Functions

The `semiring` module provides convenient factory functions:

```python
from provenance_demo.types.semiring import (
    create_why_semiring,
    create_counting_semiring,
    create_formula_semiring,
)

# Create pre-configured semirings
why = create_why_semiring()
counting = create_counting_semiring()
formula = create_formula_semiring("'t'||id")
```

---

## Migration from Old Format

**Old approach** (using string-based format):
```python
# ❌ Old way (deprecated)
DbSemiring(
    name="counting",
    mapping_format="constant",
    mapping_value="1",
    ...
)
```

**New approach** (using mapping instances):
```python
# ✅ New way
from provenance_demo.repository.provenance_mapping import ConstantMapping

DbSemiring(
    name="counting",
    mapping=ConstantMapping("1"),
    ...
)
```

**Benefits of new approach:**
- Better type safety
- No conditional logic
- Easier to extend
- Each strategy is self-contained
- Follows Open/Closed Principle

## Supported Formats

### 1. **ctid_reference** (Default)
Uses PostgreSQL's internal row identifier (ctid) to create unique references.

**Format:** `'{table_name}@p{page}r{row}'`

**Example:** `'assessment@p108r52'` refers to row at ctid (108,52) in table 'assessment'

**Usage:**
```python
from provenance_demo.types.semiring import DbSemiring

why_semiring = DbSemiring(
    name="why",
    retrieval_function="whyprov_now",
    aggregate_function="aggregation_formula",
    mapping_table="why_mapping",
    mapping_format="ctid_reference"  # Default, can be omitted
)
```

**SQL Generation:**
```python
ProvenanceMapping.get_mapping_target_sql('assessment')
# Returns: "'assessment@p'||(ctid::text::point)[0]::int||'r'||(ctid::text::point)[1]::int"
```

---

### 2. **constant**
Uses a constant value for all rows. Common for counting semirings where each row contributes the same value.

**Format:** A constant value like `'1'`

**Example:** `'1'` for all rows (counting)

**Usage:**
```python
counting_semiring = DbSemiring(
    name="counting",
    retrieval_function="counting",
    aggregate_function="counting_plus",
    mapping_table="counting_mapping",
    mapping_format="constant",
    mapping_value="1"  # Every row contributes 1
)
```

**SQL Generation:**
```python
ProvenanceMapping.get_mapping_target_sql('assessment', counting_semiring)
# Returns: "'1'"
```

**Database Setup:**
```sql
-- All rows in all tables map to '1'
SELECT create_provenance_mapping('assessment_c', 'assessment', '1');
SELECT create_provenance_mapping('questions_c', 'questions', '1');

CREATE TABLE counting_mapping AS
    SELECT * FROM assessment_c UNION
    SELECT * FROM questions_c;
```

---

### 3. **custom_expression**
Uses a custom SQL expression to generate unique identifiers. Common for formula semirings or when you want human-readable provenance IDs.

**Format:** Any SQL expression, e.g., `"'a'||id"` or `"table_name||':'||primary_key"`

**Example:** `'a123'` for assessment with id=123, `'q456'` for question with id=456

**Usage:**
```python
formula_semiring = DbSemiring(
    name="formula",
    retrieval_function="formula",
    aggregate_function="formula_plus",
    mapping_table="formula_mapping",
    mapping_format="custom_expression",
    mapping_value="'a'||id"  # 'a' prefix + id column
)
```

**SQL Generation:**
```python
ProvenanceMapping.get_mapping_target_sql('assessment', formula_semiring)
# Returns: "'a'||id"
```

**Database Setup:**
```sql
-- Each table gets its own prefix
SELECT create_provenance_mapping('assessment_f', 'assessment', '''a''||id');
SELECT create_provenance_mapping('questions_f', 'questions', '''q''||id');
SELECT create_provenance_mapping('topics_f', 'topics', '''t''||id');

CREATE TABLE formula_mapping AS
    SELECT * FROM assessment_f UNION
    SELECT * FROM questions_f UNION
    SELECT * FROM topics_f;
```

---

## Comparison Table

| Format | Use Case | Example Value | Requires ID Column | Human Readable |
|--------|----------|---------------|-------------------|----------------|
| `ctid_reference` | Why-provenance, full traceability | `'assessment@p108r52'` | No | Partially |
| `constant` | Counting, simple aggregation | `'1'` | No | Yes |
| `custom_expression` | Formula semiring, readable IDs | `'a123'`, `'q456'` | Yes (typically) | Yes |

---

## Implementation in SemiringRepository

The `SemiringRepository.enable_for()` method automatically uses the correct format:

```python
# Internally calls:
ProvenanceMapping.get_mapping_target_sql(table_name, self._semiring)

# Which returns the appropriate SQL based on semiring.mapping_format
```

---

## Backward Compatibility

If `mapping_format` is not specified, it defaults to `'ctid_reference'` for backward compatibility with existing code.

```python
# These are equivalent:
semiring1 = DbSemiring(name="why", mapping_format="ctid_reference", ...)
semiring2 = DbSemiring(name="why", ...)  # Defaults to ctid_reference
```
