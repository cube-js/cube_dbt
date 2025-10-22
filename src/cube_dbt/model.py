from cube_dbt.column import Column
from cube_dbt.dump import dump, SafeString

class Model:
  def __init__(self, model_dict: dict, test_index: dict = None) -> None:
    self._model_dict = model_dict
    self._test_index = test_index or {}
    self._columns = None
    self._primary_key = None
    self._constraint_primary_keys = None
    pass

  def __repr__(self) -> str:
    return str(self._model_dict)

  def _init_columns(self) -> None:
    if self._columns == None:
      self._columns = list(
        Column(self.name, column, self._test_index.get(column['name'], []))
        for key, column in self._model_dict['columns'].items()
      )
      self._detect_constraint_primary_keys()
      self._detect_primary_key()

  def _detect_constraint_primary_keys(self) -> None:
    """
    Detect primary keys defined via dbt 1.5+ constraints.
    Example: constraints: [{type: 'primary_key', columns: ['id']}]
    """
    self._constraint_primary_keys = []
    constraints = self._model_dict.get('constraints', [])

    for constraint in constraints:
      if constraint.get('type') == 'primary_key':
        columns = constraint.get('columns', [])
        self._constraint_primary_keys.extend(columns)

  def _detect_primary_key(self) -> None:
    """
    Detect primary keys from multiple sources:
    1. Constraint-based primary keys (dbt 1.5+)
    2. Column tags (legacy cube_dbt approach)
    3. unique + not_null tests (standard dbt approach)
    """
    primary_keys = []

    # First check for constraint-based primary keys
    if self._constraint_primary_keys:
      for column in self._columns:
        if column.name in self._constraint_primary_keys:
          primary_keys.append(column)
    else:
      # Fall back to tag-based or test-based detection
      primary_keys = [
        column for column in self._columns
        if column.primary_key
      ]

    self._primary_key = primary_keys
  
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
  
  def column(self, name: str) -> Column:
    self._init_columns()
    return next(column for column in self._columns if column.name == name)
  
  @property
  def primary_key(self) -> list[Column]:
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
  
  def _as_dimensions(self, skip: list[str]=[]) -> list:
    return list(
      column._as_dimension()
      for column in self.columns
      if column.name not in skip
    )
  
  def as_dimensions(self, skip: list[str]=[]) -> str:
    """
    For use in Jinja:
    {{ dbt.model('name').as_dimensions(skip=['id']) }}
    """
    dimensions = self._as_dimensions(skip)
    return dump(dimensions, indent=6) if dimensions else SafeString('')