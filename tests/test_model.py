from pytest import raises
from cube_dbt import Model

class TestModel:
  def test_sql_table_with_relation(self):
    """
    If a relation is present, then use it
    """
    model_dict = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2'
    }
    model = Model(model_dict)
    assert model.sql_table == '"db"."schema"."table"'

  def test_sql_table_without_relation(self):
    """
    If a relation is not present, then compose it
    """
    model_dict = {
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2'
    }
    model = Model(model_dict)
    assert model.sql_table == '`db_2`.`schema_2`.`table_2`'

  def test_sql_table_without_relation_with_alias(self):
    """
    If a relation is not present and an alias is,
    then compose it using that alias 
    """
    model_dict = {
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'alias': 'alias_2'
    }
    model = Model(model_dict)
    assert model.sql_table == '`db_2`.`schema_2`.`alias_2`'

  def test_detect_primary_key_no_columns(self):
    """
    If a model has no columns with a 'primary key'
    tag, then detect no primary keys
    """
    model_dict = {
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
    model = Model(model_dict)
    assert model.primary_key == None

  def test_detect_primary_key_one_column(self):
    """
    If a model has one and only one column with a 'primary key'
    tag, then use it as a primary key
    """
    model_dict = {
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
    model = Model(model_dict)
    assert model.primary_key != None
    assert model.primary_key.name == 'id'

  def test_detect_primary_key_two_columns(self):
    """
    If a model has more than one column with a 'primary_key'
    tag, then raise an exception
    """
    model_dict = {
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
    model = Model(model_dict)
    with raises(RuntimeError):
      model.primary_key

  def test_as_cube(self):
    model_dict = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'description': ''
    }
    model = Model(model_dict)
    assert model._as_cube() == {
      'name': 'table_2',
      'sql_table': '"db"."schema"."table"'
    }

  def test_as_cube_render(self):
    model_dict = {
      'relation_name': '"db"."schema"."table"',
      'database': 'db_2',
      'schema': 'schema_2',
      'name': 'table_2',
      'description': ''
    }
    model = Model(model_dict)
    assert model.as_cube() == """name: table_2
    sql_table: '"db"."schema"."table"'
    """

  def test_as_dimensions(self):
    model_dict = {
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
    model = Model(model_dict)
    assert model._as_dimensions() == [
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
    model_dict = {
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
    model = Model(model_dict)
    assert model._as_dimensions() == [
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
    model_dict = {
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
    model = Model(model_dict)
    assert model.as_dimensions() == """- name: id
      sql: id
      type: number
      primary_key: true
    - name: status
      sql: status
      type: string
    """