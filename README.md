# cube_dbt

`cube_dbt` is the dbt integration for [Cube](https://cube.dev) that helps define the data model of the semantic layer on top of dbt models.

## Installation

```sh
pip install cube_dbt
```

## Usage

```python
manifest_url = 'https://bucket.s3.amazonaws.com/manifest.json'
dbt = (
  Dbt
  .from_url(manifest_url)
  .filter(
    paths=['marts/'],
    tags=['cube'],
    names=['my_table', 'my_table_2']
  )
)
print(dbt.models)

# For use in Jinja templates:
print(dbt.model('name').as_cube())
print(dbt.model('name').as_dimensions(skip=['id']))
print(dbt.model('name').column('name').as_dimension())
```

## Development

Run tests:

```sh
pdm run test
```

## Preprocessing the `manifest.json` file

In case of a massive manifest file, it can be preprocessed for optimal performance. The `cube_dbt` package only reads the `nodes` dictionary where `resource_type` is `model`.
Here's a list of all keys used by the `cube_dbt` package:

``` 
- nodes
  - name
  - path
  - description
  - config
    - materialized
    - tags
  - resource_type
  - database
  - schema
  - alias
  - relation_name
  - columns
    - name
    - description
    - data_type
    - meta
    - tags
```
