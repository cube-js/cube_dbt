from cube_dbt.dump import dump

Column = dict

def column_name(column: Column) -> str:
  return column['name']

def column_description(column: Column) -> str:
  return column['description']

def column_sql(column: Column) -> str:
  return column['name']

def column_type(column: Column) -> str:
  if not 'data_type' in column or column['data_type'] == None:
    return 'string'
  
  column_to_dimension_types = {
    'bool': 'boolean',
    'date': 'time',
    'datetime': 'time',
    'geography': 'geo',
    'numeric': 'number',
    'string': 'string',
    'timestamp': 'time'
  }
  if not column['data_type'] in column_to_dimension_types:
    raise RuntimeError(f"Unknown type of column '{column_name(column)}': {column['data_type']}")

  return column_to_dimension_types[column['data_type']]

def column_meta(column: Column) -> dict:
  return column['meta']

def column_primary_key(column: Column) -> bool:
  """
  Convention: if the column is marked with the 'primary_key' tag,
  it will be mapped to a primary key dimension
  """
  return 'primary_key' in column['tags']

def _column_as_dimension(column: Column) -> dict:
  data = {}
  
  data['name'] = column_name(column)

  description = column_description(column)
  if description:
    data['description'] = description

  data['sql'] = column_sql(column)

  data['type'] = column_type(column)

  primary_key = column_primary_key(column)
  if primary_key:
    data['primary_key'] = primary_key

  meta = column_meta(column)
  if meta:
    data['meta'] = meta

  return data

def column_as_dimension(column: Column) -> str:
  """
  For use in Jinja
  """
  return dump(_column_as_dimension(column), indent=8)