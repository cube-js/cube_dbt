from cube_dbt.column import (
  Column,
  column_name,
  column_primary_key,
  _column_as_dimension
)
from cube_dbt.dump import dump

Model = dict

def model_name(model: Model) -> str:
  return model['name']

def model_description(model: Model) -> str:
  return model['description']

def model_sql_table(model: Model) -> str:
  if 'relation_name' in model:
    return model['relation_name']
  else:
    name = model['alias'] if 'alias' in model else model['name']
    return f"`{model['database']}`.`{model['schema']}`.`{name}`"

def model_columns(model: Model) -> list[Column]:
  return list(model for key, model in model['columns'].items())

def model_column(model: Model, name: str) -> Column:
  return next(
    column for column in model_columns(model)
    if column_name(column) == name
  )

def model_primary_key(model: Model) -> None:
  candidates = list(
    column for column in model_columns(model)
    if column_primary_key(column)
  )

  if len(candidates) > 1:
    column_names = list(column_name(column) for column in candidates)
    raise RuntimeError(f"More than one primary key column found in model '{model_name(model)}': {', '.join(column_names)}")

  return candidates[0] if len(candidates) == 1 else None

def _model_as_cube(model: Model) -> dict:
  data = {}

  data['name'] = model_name(model)

  description = model_description(model)
  if description:
    data['description'] = description

  data['sql_table'] = model_sql_table(model)

  return data

def model_as_cube(model: Model) -> str:
  """
  For use in Jinja
  """
  return dump(_model_as_cube(model), indent=4)

def _model_as_dimensions(model: Model) -> list[dict]:
  return list(
    _column_as_dimension(column)
    for column in model_columns(model)
  )

def model_as_dimensions(model: Model) -> str:
  """
  For use in Jinja
  """
  return dump(_model_as_dimensions(model), indent=6)