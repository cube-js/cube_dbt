# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**cube_dbt** is a Python package that converts dbt models and columns into Cube semantic layer definitions. It parses dbt manifest files and provides Jinja-compatible YAML output for integrating data models with Cube's semantic layer.

## Common Development Commands

```bash
# Testing
pdm run test                 # Run all tests (34 unit tests)
pytest tests/ -v             # Run tests with verbose output
pytest tests/test_dbt.py     # Run specific test file
pytest -k "test_model"       # Run tests matching pattern

# Development Setup
pdm install                  # Install project with dev dependencies
pdm install --prod           # Install production dependencies only
pdm lock                     # Update pdm.lock file
pdm update                   # Update all dependencies

# Building & Publishing
pdm build                    # Build distribution packages
pdm publish                  # Publish to PyPI (requires credentials)

# Development Workflow
pdm run python -m cube_dbt   # Run the module directly
python -c "from cube_dbt import Dbt; print(Dbt.version())"  # Check version
```

## High-Level Architecture

The package consists of 4 core classes that work together:

### Core Classes

**Dbt (src/cube_dbt/dbt.py)**
- Entry point for loading dbt manifest files
- Supports file paths and URLs via `from_file()` and `from_url()` class methods
- Implements chainable filtering API: `filter(paths=[], tags=[], names=[])`
- Lazy initialization - models are only loaded when accessed
- Handles manifest v1-v12 formats

**Model (src/cube_dbt/model.py)**
- Represents a single dbt model from the manifest
- Key method: `as_cube()` - exports model as Cube-compatible YAML
- Supports multiple primary keys via column tags
- Provides access to columns, description, database, schema, and alias
- Handles special characters in model names (spaces, dots, dashes)

**Column (src/cube_dbt/column.py)**
- Represents dbt columns with comprehensive type mapping
- Maps 130+ database-specific types to 5 Cube dimension types:
  - string, number, time, boolean, geo
- Database support: BigQuery, Snowflake, Redshift, generic SQL
- Primary key detection via `primary_key` tag in column metadata
- Raises RuntimeError for unknown column types (fail-fast approach)

**Dump (src/cube_dbt/dump.py)**
- Custom YAML serialization utilities
- Returns Jinja SafeString for template compatibility
- Handles proper indentation for nested structures
- Used internally by Model.as_cube() for output formatting

### Key Design Patterns

1. **Lazy Loading**: Models are loaded only when first accessed via `dbt.models` property
2. **Builder Pattern**: Filter methods return self for chaining: `dbt.filter(tags=['tag1']).filter(paths=['path1'])`
3. **Factory Methods**: `Dbt.from_file()` and `Dbt.from_url()` for different data sources
4. **Type Mapping Strategy**: Centralized database type to Cube type conversion in Column class

### Data Flow

```
manifest.json → Dbt.from_file() → filter() → models → Model.as_cube() → YAML output
                                                ↓
                                            columns → Column.dimension_type()
```

## Testing Structure

Tests use a real dbt manifest fixture (tests/manifest.json, ~397KB) with example models:

- **test_dbt.py**: Tests manifest loading, filtering by paths/tags/names, version checking
- **test_model.py**: Tests YAML export, primary key handling, special character escaping
- **test_column.py**: Tests type mapping for different databases, primary key detection
- **test_dump.py**: Tests YAML formatting and Jinja compatibility

Run specific test scenarios:
```bash
pytest tests/test_column.py::TestColumn::test_bigquery_types -v
pytest tests/test_model.py::TestModel::test_multiple_primary_keys -v
```

## Important Implementation Details

### Primary Key Configuration
Primary keys are defined using tags in dbt column metadata:
```yaml
# In dbt schema.yml
columns:
  - name: id
    meta:
      tags: ['primary_key']
```

### Type Mapping Behavior
- Unknown types raise RuntimeError immediately (fail-fast)
- Database-specific types are checked first, then generic SQL types
- Default mappings can be found in `src/cube_dbt/column.py` TYPE_MAP dictionaries

### Jinja Template Integration
All output from `as_cube()` is wrapped in Jinja SafeString to prevent double-escaping in templates. Use the `safe` filter if needed in templates.

### URL Loading Authentication
When using `Dbt.from_url()`, basic authentication is supported:
```python
dbt = Dbt.from_url("https://user:pass@example.com/manifest.json")
```

## Recent Changes (from git history)

- Multiple primary key support (#15)
- Documentation of package properties (#14)
- Extended dbt contract data type support (#10)
- Jinja escaping protection for as_cube() (#2)

## Package Metadata

- **Version**: Defined in `src/cube_dbt/__init__.py`
- **Python Requirement**: >= 3.8
- **Production Dependency**: PyYAML >= 6.0.1
- **License**: MIT
- **Build System**: PDM with PEP 517/518 compliance