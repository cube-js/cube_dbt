from pytest import raises
from cube_dbt import (
  Column,
  column_type,
  column_as_dimension
)
from cube_dbt.column import _column_as_dimension

class TestColumn:
  def test_no_type(self):
    """
    If no type, then assume 'string'
    """
    column = {}
    assert column_type(column) == 'string'

  def test_none_type(self):
    """
    If type is None, then assume 'string'
    """
    column = {
      'data_type': None
    }
    assert column_type(column) == 'string'

  def test_unknown_type(self):
    """
    If type is unknown, then raise an exception
    """
    column = {
      'name': 'column',
      'data_type': 'unknown'
    }
    with raises(RuntimeError):
      column_type(column)

  def test_known_type(self):
    """
    If type is known, then map it
    """
    column = {
      'data_type': 'numeric'
    }
    assert column_type(column) == 'number'

  def test_as_dimension(self):
    column = {
      'name': 'column',
      'description': '',
      'meta': {},
      'data_type': 'numeric',
      'tags': []
    }
    assert _column_as_dimension(column) == {
      'name': 'column',
      'sql': 'column',
      'type': 'number'
    }

  def test_as_dimension_render(self):
    column = {
      'name': 'column',
      'description': '',
      'meta': {},
      'data_type': 'numeric',
      'tags': []
    }
    assert column_as_dimension(column) == """name: column
        sql: column
        type: number
        """