import yaml

class Dumper(yaml.Dumper):
  def increase_indent(self, flow=False, indentless=False):
    return super(Dumper, self).increase_indent(flow, indentless)
    
def indent_string(string: str, indent: int) -> str:
  return '\n'.join(
    (' ' * indent if i > 0 else '') +
    s for i, s in enumerate(string.split('\n'))
  )

def dump(data, indent: int=0) -> str:
  dump = yaml.dump(
    data,
    Dumper=Dumper,
    sort_keys=False,
    default_flow_style=False,
    allow_unicode=True,
    indent=0
  )
  return indent_string(dump, indent)