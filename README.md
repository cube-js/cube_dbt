# cube_dbt

`cube_dbt` is the dbt integration for [Cube](https://cube.dev) that helps define the data model of the semantic layer on top of dbt models.

## Installation

```sh
pip install cube_dbt
```

## Usage

```python
manifest_url = 'https://bucket.s3.amazonaws.com/manifest.json'
dbt = Dbt
  .from_url(manifest_url)
  .filter(
    path_prefix='marts/',
    tags=['cube'],
    names=['my_table', 'my_table_2']
  )
print(dbt.models)

# For use in Jinja templates:
print(dbt.model('name').as_cube())
print(dbt.model('name').as_dimensions())
print(dbt.model('name').column('name').as_dimension())
```

## Development

Run tests:

```sh
pdm run test
```