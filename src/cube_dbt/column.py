import re

from cube_dbt.dump import dump

# As of 2024-10-17, the valid "Dimension Types" listed on
# https://cube.dev/docs/reference/data-model/types-and-formats#dimension-types
# are: time, string, number, boolean, and geo
VALID_DIMENSION_TYPES = [
  "boolean",
  "geo",
  "number",
  "string",
  "time",
]
# Other System's Type => Cube Type
# See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
BIGQUERY_TYPE_MAPPINGS = {
  "bool": "boolean",
}
# See https://docs.snowflake.com/en/sql-reference-data-types
SNOWFLAKE_TYPE_MAPPINGS = {
  # Numeric data types
  # number does not need to be mapped
  "decimal": "number",
  "dec": "number",
  "numeric": "number",
  "int": "number",
  "integer": "number",
  "bigint": "number",
  "smallint": "number",
  "tinyint": "number",
  "byteint": "number",
  "float": "number",
  "float4": "number",
  "float8": "number",
  "double": "number",
  "double precision": "number",
  "real": "number",

  # String & binary data types
  "varchar": "string",
  "char": "string",
  "character": "string",
  "nchar": "string",
  # string does not need to be mapped
  "text": "string",
  "nvarchar": "string",
  "nvarchar2": "string",
  "char varying": "string",
  "nchar varying": "string",
  "binary": "string",
  "varbinary": "string",

  # Logical data types
  # boolean does not need to be mapped

  # Date & time data types
  "date": "time",
  "datetime": "time",
  # time does not need to be mapped
  "timestamp": "time",
  "timestamp_ltz": "time",
  "timestamp_ntz": "time",
  "timestamp_tz": "time",

  # Semi-structured data types
  "variant": "string",
  "object": "string",
  "array": "string",

  # Geospatial data types
  "geography": "geo",
  "geometry": "string",

  # Vector data types
  "vector": "string",
}
TYPE_MAPPINGS = {
  **BIGQUERY_TYPE_MAPPINGS,
  **SNOWFLAKE_TYPE_MAPPINGS,
}


class Column:
  def __init__(self, model_name: str, column_dict: dict) -> None:
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

    # Normalize the data_type value, downcasing it, and removing extra information.
    source_data_type = re.sub(r"\([^\)]*\)", "", self._column_dict["data_type"].lower())

    if source_data_type in TYPE_MAPPINGS:
      cube_data_type = TYPE_MAPPINGS[source_data_type]
    else:
      cube_data_type = source_data_type

    if cube_data_type not in VALID_DIMENSION_TYPES:
      raise RuntimeError(f"Unknown column type of {self._model_name}.{self.name}: {self._column_dict['data_type']}")

    return cube_data_type

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
