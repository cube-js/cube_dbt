from cube_dbt.dump import dump

class Column:
  def __init__(self, model_name: str, column_dict) -> None:
    self._model_name = model_name
    self._column_dict = column_dict
    pass
  
  def __repr__(self) -> str:
    return str(self._column_dict)
  
  @property
  def name(self) -> str:
    return self._column_dict['name']
  
  @property
  def description(self) -> str:
    return self._column_dict['description']
  
  @property
  def sql(self) -> str:
    return self._column_dict['name']
  
  @property
  def type(self) -> str:
    if not 'data_type' in self._column_dict or self._column_dict['data_type'] == None:
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
    if not self._column_dict['data_type'] in column_to_dimension_types:
      raise RuntimeError(f"Unknown column type of {self._model_name}.{self.name}: {self._column_dict['data_type']}")

    return column_to_dimension_types[self._column_dict['data_type']]
  
  @property
  def meta(self) -> dict:
    return self._column_dict['meta']
  
  @property
  def primary_key(self) -> bool:
    """
    Convention: if the column is marked with the 'primary_key' tag,
    it will be mapped to a primary key dimension
    """
    return 'primary_key' in self._column_dict['tags']

  def _as_dimension(self) -> dict:
    data = {}
    data['name'] = self.name
    if self.description:
      data['description'] = self.description
    data['sql'] = self.sql
    data['type'] = self.type
    if self.primary_key:
      data['primary_key'] = True
    if self.meta:
      data['meta'] = self.meta
    return data
  
  def as_dimension(self) -> str:
    """
    For use in Jinja:
    {{ dbt.model('name').column('name').as_dimension() }}
    """
    return dump(self._as_dimension(), indent=8)