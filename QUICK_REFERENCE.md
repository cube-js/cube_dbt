# cube_dbt Quick Reference

## What is cube_dbt?
A Python package that converts dbt models and columns into Cube semantic layer definitions. It parses dbt manifests and provides Jinja-compatible YAML output.

## Install & Run Tests
```bash
pdm install              # Set up environment
pdm run test             # Run all tests
```

## Basic Usage
```python
from cube_dbt import Dbt

# Load and filter
dbt = Dbt.from_file('manifest.json').filter(
    paths=['marts/'],
    tags=['cube'],
    names=['model_name']
)

# Access models
model = dbt.model('my_model')
print(model.name)
print(model.sql_table)
print(model.columns)

# Export to Cube (YAML)
print(model.as_cube())
print(model.as_dimensions())
```

## Project Structure
```
src/cube_dbt/
├── dbt.py         - Dbt class (manifest loading & filtering)
├── model.py       - Model class (cube export)
├── column.py      - Column class (type mapping)
├── dump.py        - YAML utilities (Jinja-safe)
└── __init__.py    - Public exports

tests/             - 34 unit tests, all passing
```

## Key Classes

### Dbt
- `from_file(path)` - Load from JSON
- `from_url(url)` - Load from remote URL
- `filter(paths=[], tags=[], names=[])` - Chainable filtering
- `.models` - Get all filtered models
- `.model(name)` - Get single model

### Model
- `.name`, `.description`, `.sql_table` - Properties
- `.columns` - List of Column objects
- `.primary_key` - List of primary key columns
- `.as_cube()` - Export as Cube definition (YAML)
- `.as_dimensions()` - Export dimensions (YAML)

### Column
- `.name`, `.description`, `.type`, `.meta` - Properties
- `.primary_key` - Boolean
- `.as_dimension()` - Export dimension (YAML)

Type mapping: BigQuery, Snowflake, Redshift → Cube types (number, string, time, boolean, geo)

## Dependencies
- Production: PyYAML >= 6.0.1
- Development: pytest >= 7.4.2
- Python: >= 3.8

## Common Tasks
| Task | Command |
|------|---------|
| Run tests | `pdm run test` |
| Run specific test | `pytest tests/test_dbt.py -v` |
| Install deps | `pdm install` |
| Lock deps | `pdm lock` |
| Build package | `pdm build` |

## Recent Changes
- v0.6.2: Multiple primary keys support
- Type support for dbt contracts
- Jinja template safe rendering

## Publishing
GitHub Actions auto-publishes to PyPI on release.
