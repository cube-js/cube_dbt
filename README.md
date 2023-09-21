# cube_dbt

`cube_dbt` is the dbt integration for [Cube](https://cube.dev) that helps define the data model of the semantic layer on top of dbt models.

## Installation

```sh
pip install cube_dbt
```

## Usage

```python
manifest_url = 'https://bucket.s3.amazonaws.com/manifest.json'
manifest = load_dbt_manifest_from_url(manifest_url)
models = dbt_models(
  manifest,
  path_prefix='marts/',
  tags=['cube'],
  names=['my_table', 'my_table_2']
)
print(models)

# For use in Jinja templates:
model = dbt_model(models, 'name')
print(model_as_cube(model))
print(model_as_dimensions(model))

column = model_column(model, 'name')
print(column_as_dimension(column))
```

## Development

Run tests:

```sh
pdm run test
```