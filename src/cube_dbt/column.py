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
  "array": "string",
  "bool": "boolean",
  "bytes": "string",
  "date": "time",
  "datetime": "time",
  "geography": "geo",
  "interval": "string",
  "json": "string",

  # numeric types
  "int64": "number",
  "int": "number",
  "smallint": "number",
  "integer": "number",
  "bigint": "number",
  "tinyint": "number",
  "byteint": "number",
  "numeric": "number",
  "decimal": "number",
  "bignumeric": "number",
  "bigdecimal": "number",
  "float64": "number",

  "range": "string",
  # string does not need to be mapped
  "struct": "string",
  # time does not need to be mapped
  "timestamp": "time",
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
# See https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
REDSHIFT_TYPE_MAPPINGS = {
  # Signed two-byte integer
  "smallint": "number",
  "int2": "number",

  # Signed four-byte integer
  "integer": "number",
  "int": "number",
  "int4": "number",

  # Signed eight-byte integer
  "bigint": "number",
  "int8": "number",

  # Exact numeric of selectable precision
  "decimal": "number",
  "numeric": "number",

  # Single precision floating-point number
  "real": "number",
  "float4": "number",

  # Double precision floating-point number
  "double precision": "number",
  "float8": "number",
  "float": "number",

  # Fixed-length character string
  "char": "string",
  "character": "string",
  "nchar": "string",
  "bpchar": "string",

  # Variable-length character string with a user-defined limit
  "varchar": "string",
  "character varying": "string",
  "nvarchar": "string",
  "text": "string",

  # Calendar date (year, month, day)
  "date": "time",

  # Time of day
  "time": "time",
  "time without time zone": "time",

  # Time of day with time zone
  "timetz": "time",
  "time with time zone": "time",

  # Date and time (without time zone)
  "timestamp": "time",
  "timestamp without time zone": "time",

  # Date and time (with time zone)
  "timestamptz": "time",
  "timestamp with time zone": "time",

  # Time duration in year to month order
  "interval year to month": "string",

  # Time duration in day to second order
  "interval day to second": "string",

  # Logical Boolean (true/false)
  # boolean does not need to be mapped
  "bool": "boolean",

  # Type used with HyperLogLog sketches
  "hllsketch": "string",

  # A superset data type that encompasses all scalar types of Amazon Redshift including complex types such as ARRAY and STRUCTS
  "super": "string",

  # Variable-length binary value
  "varbyte": "string",
  "varbinary": "string",
  "binary varying": "string",

  # Spatial data
  "geometry": "geo",
  "geography": "string",
}
TYPE_MAPPINGS = {
  **BIGQUERY_TYPE_MAPPINGS,
  **REDSHIFT_TYPE_MAPPINGS,
  **SNOWFLAKE_TYPE_MAPPINGS,
}


class Column:
  def __init__(self, model_name: str, column_dict: dict, tests: list = None) -> None:
    self._model_name = model_name
    self._column_dict = column_dict
    self._tests = tests or []
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
    source_data_type = re.sub(r"<.*>", "", re.sub(r"\([^\)]*\)", "", self._column_dict["data_type"].lower()))

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
    Detects if a column is a primary key using multiple methods:
    1. Column tagged with 'primary_key' tag (legacy cube_dbt convention)
    2. Column has both 'unique' and 'not_null' tests (standard dbt convention)
    3. Column has 'primary_key' constraint (checked at model level)
    """
    # Method 1: Check for 'primary_key' tag (legacy approach)
    if 'primary_key' in self._column_dict['tags']:
      return True

    # Method 2: Check for unique + not_null tests (standard dbt approach)
    if 'unique' in self._tests and 'not_null' in self._tests:
      return True

    return False

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
