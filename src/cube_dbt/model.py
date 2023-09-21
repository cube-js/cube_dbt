from cube_dbt.column import Column
from cube_dbt.dump import dump

class Model:
  def __init__(self, model_dict) -> None:
    self._model_dict = model_dict
    self._columns = None
    self._primary_key = None
    pass

  def __repr__(self) -> str:
    return str(self._model_dict)

  def _init_columns(self) -> None:
    if self._columns == None:
      self._columns = list(
        Column(self.name, column) for key, column in self._model_dict['columns'].items()
      )
      self._detect_primary_key()

  def _detect_primary_key(self) -> None:
    candidates = list(
      column for column in self._columns
      if column.primary_key
    )

    if len(candidates) > 1:
      column_names = list(column.name for column in candidates)
      raise RuntimeError(f"More than one primary key column found in {self.name}: {', '.join(column_names)}")

    self._primary_key = candidates[0] if len(candidates) == 1 else None
  
  @property
  def name(self) -> str:
    return self._model_dict['name']
  
  @property
  def description(self) -> str:
    return self._model_dict['description']
  
  @property
  def sql_table(self) -> str:
    if 'relation_name' in self._model_dict:
      return self._model_dict['relation_name']
    else:
      database = self._model_dict['database']
      schema = self._model_dict['schema']
      name = self._model_dict['alias'] if 'alias' in self._model_dict else self._model_dict['name']
      return f"`{database}`.`{schema}`.`{name}`"

  @property
  def columns(self) -> list[Column]:
    self._init_columns()
    return self._columns
  
  def column(self, name) -> Column:
    self._init_columns()
    return next(column for column in self._columns if column.name == name)
  
  @property
  def primary_key(self) -> Column or None:
    self._init_columns()
    return self._primary_key

  def _as_cube(self) -> dict:
    data = {}
    data['name'] = self.name
    if self.description:
      data['description'] = self.description
    data['sql_table'] = self.sql_table
    return data

  def as_cube(self) -> str:
    """
    For use in Jinja:
    {{ dbt.model('name').as_cube() }}
    """
    return dump(self._as_cube(), indent=4)
  
  def _as_dimensions(self) -> list:
    return list(
      column._as_dimension()
      for column in self.columns
    )
  
  def as_dimensions(self) -> str:
    """
    For use in Jinja:
    {{ dbt.model('name').as_dimensions() }}
    """
    return dump(self._as_dimensions(), indent=4)