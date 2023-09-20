from cube_dbt.dump import indent_string

class TestIndentString:
  def test_single_line_string(self):
    """
    First line is not indented
    """
    input = 'abc'
    output = indent_string(input, 2)
    assert output == 'abc'

  def test_milti_line_string(self):
    """
    First line is not indented, all other lines are indented
    """
    input = 'abc\ndef\nghi'
    output = indent_string(input, 2)
    assert output == 'abc\n  def\n  ghi'