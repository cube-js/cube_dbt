from pytest import raises
from cube_dbt import Column

class TestColumn:
  def test_no_type(self):
    """
    If no type, then assume 'string'
    """
    column_dict = {}
    column = Column('model', column_dict)
    assert column.type == 'string'

  def test_none_type(self):
    """
    If type is None, then assume 'string'
    """
    column_dict = {
      'data_type': None
    }
    column = Column('model', column_dict)
    assert column.type == 'string'

  def test_unknown_type(self):
    """
    If type is unknown, then raise an exception
    """
    column_dict = {
      'name': 'column',
      'data_type': 'unknown'
    }
    column = Column('model', column_dict)
    with raises(RuntimeError):
      column.type

  def test_known_type(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'numeric'
    }
    column = Column('model', column_dict)
    assert column.type == 'number'

  def test_known_type_but_uppercase(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'STRING'
    }
    column = Column('model', column_dict)
    assert column.type == 'string'

  def test_known_type_but_with_one_extra_info(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'timestamp(3)'
    }
    column = Column('model', column_dict)
    assert column.type == 'time'

  def test_known_type_but_with_two_extra_info(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'numeric(38,0)'
    }
    column = Column('model', column_dict)
    assert column.type == 'number'

  def test_known_type_but_with_two_extra_info_of_different_types(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'VECTOR(FLOAT, 256)'
    }
    column = Column('model', column_dict)
    assert column.type == 'string'

  def test_known_bigquery_type_but_with_extra_info(self):
    """
    If type is known, then map it
    """
    column_dict = {
      'data_type': 'ARRAY<STRUCT<ARRAY<INT64>>>'
    }
    column = Column('model', column_dict)
    assert column.type == 'string'

  def test_as_dimension(self):
    column_dict = {
      'name': 'column',
      'description': '',
      'meta': {},
      'data_type': 'numeric',
      'tags': []
    }
    column = Column('model', column_dict)
    assert column._as_dimension() == {
      'name': 'column',
      'sql': 'column',
      'type': 'number'
    }

  def test_as_dimension_render(self):
    column_dict = {
      'name': 'column',
      'description': '',
      'meta': {},
      'data_type': 'numeric',
      'tags': []
    }
    column = Column('model', column_dict)
    assert column.as_dimension() == """name: column
        sql: column
        type: number
        """
