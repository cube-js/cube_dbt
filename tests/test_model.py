from pytest import raises
from cube_dbt import (
  Model,
  model_name,
  model_sql_table,
  model_primary_key,
  model_as_cube,
  model_as_dimensions
)
from cube_dbt.model import (
  _model_as_cube,
  _model_as_dimensions
)

class TestModel:
  def test_sql_table_with_relation(self):
    """
    If a relation is present, then use it
    """
    model = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2'
    }
    assert model_sql_table(model) == '"db"."schema"."table"'

  def test_sql_table_without_relation(self):
    """
    If a relation is not present, then compose it
    """
    model = {
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2'
    }
    assert model_sql_table(model) == '`db_2`.`schema_2`.`table_2`'

  def test_sql_table_without_relation_with_alias(self):
    """
    If a relation is not present and an alias is,
    then compose it using that alias 
    """
    model = {
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'alias': 'alias_2'
    }
    assert model_sql_table(model) == '`db_2`.`schema_2`.`alias_2`'

  def test_detect_primary_key_no_columns(self):
    """
    If a model has no columns with a 'primary key'
    tag, then detect no primary keys
    """
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'data_type': 'numeric',
          'tags': []
        },
        'status': {
          'name': 'status',
          'data_type': None,
          'tags': []
        }
      }
    }
    assert model_primary_key(model) == None

  def test_detect_primary_key_one_column(self):
    """
    If a model has one and only one column with a 'primary key'
    tag, then use it as a primary key
    """
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'data_type': 'numeric',
          'tags': [
            'primary_key'
          ]
        },
        'status': {
          'name': 'status',
          'data_type': None,
          'tags': []
        }
      }
    }
    primary_key = model_primary_key(model)
    assert primary_key != None
    assert model_name(primary_key) == 'id'

  def test_detect_primary_key_two_columns(self):
    """
    If a model has more than one column with a 'primary_key'
    tag, then raise an exception
    """
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'data_type': 'numeric',
          'tags': [
            'primary_key'
          ]
        },
        'status': {
          'name': 'status',
          'data_type': None,
          'tags': [
            'primary_key'
          ]
        }
      }
    }
    with raises(RuntimeError):
      model_primary_key(model)

  def test_as_cube(self):
    model = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'description': ''
    }
    assert _model_as_cube(model) == {
      'name': 'table_2',
      'sql_table': '"db"."schema"."table"'
    }

  def test_as_cube_render(self):
    model = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'description': ''
    }
    assert model_as_cube(model) == """name: table_2
    sql_table: '"db"."schema"."table"'
    """

  def test_as_dimensions(self):
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'description': '',
          'meta': {},
          'data_type': 'numeric',
          'tags': []
        },
        'status': {
          'name': 'status',
          'description': '',
          'meta': {},
          'data_type': None,
          'tags': []
        }
      }
    }
    assert _model_as_dimensions(model) == [
      {
        'name': 'id',
        'sql': 'id',
        'type': 'number'
      },
      {
        'name': 'status',
        'sql': 'status',
        'type': 'string'
      }
    ]

  def test_as_dimensions_with_primary_key(self):
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'description': '',
          'meta': {},
          'data_type': 'numeric',
          'tags': [
            'primary_key'
          ]
        },
        'status': {
          'name': 'status',
          'description': '',
          'meta': {},
          'data_type': None,
          'tags': []
        }
      }
    }
    assert _model_as_dimensions(model) == [
      {
        'name': 'id',
        'sql': 'id',
        'type': 'number',
        'primary_key': True
      },
      {
        'name': 'status',
        'sql': 'status',
        'type': 'string'
      }
    ]

  def test_as_dimensions_render(self):
    model = {
      'name': 'model',
      'columns': {
        'id': {
          'name': 'id',
          'description': '',
          'meta': {},
          'data_type': 'numeric',
          'tags': [
            'primary_key'
          ]
        },
        'status': {
          'name': 'status',
          'description': '',
          'meta': {},
          'data_type': None,
          'tags': []
        }
      }
    }
    assert model_as_dimensions(model) == """- name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      """