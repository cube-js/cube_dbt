try:
    import orjson as json
    # orjson.loads() requires bytes, returns dict
    _USE_ORJSON = True
except ImportError:
    import json
    _USE_ORJSON = False

from urllib.request import urlopen
from cube_dbt.model import Model
    
class Dbt:
  def __init__(self, manifest: dict) -> None:
    self.manifest = manifest
    self.paths = ''
    self.tags = []
    self.names = []
    self._models = None
    self._test_index = None
    pass

  @staticmethod
  def from_file(manifest_path: str) -> 'Dbt':
    if _USE_ORJSON:
      with open(manifest_path, 'rb') as file:
        manifest = json.loads(file.read())
    else:
      with open(manifest_path, 'r', encoding='utf-8') as file:
        manifest = json.load(file)
    return Dbt(manifest)

  @staticmethod
  def from_url(manifest_url: str) -> 'Dbt':
    with urlopen(manifest_url) as file:
      data = file.read()
      if _USE_ORJSON:
        manifest = json.loads(data)
      else:
        manifest = json.loads(data.decode('utf-8'))
    return Dbt(manifest)
    
  def filter(self, paths: list[str]=[], tags: list[str]=[], names: list[str]=[]) -> 'Dbt':
    self.paths = paths
    self.tags = tags
    self.names = names
    return self

  def _build_test_index(self):
    """
    Build an index of tests by model and column for efficient lookup.
    Returns a dict like:
    {
      'model_name': {
        'column_name': ['unique', 'not_null', ...]
      }
    }
    """
    if self._test_index is not None:
      return

    self._test_index = {}

    for key, node in self.manifest.get('nodes', {}).items():
      if node.get('resource_type') != 'test':
        continue

      test_metadata = node.get('test_metadata')
      if not test_metadata:
        continue

      test_name = test_metadata.get('name')
      kwargs = test_metadata.get('kwargs', {})
      column_name = kwargs.get('column_name')

      if not test_name or not column_name:
        continue

      # Get the model this test depends on
      depends_on = node.get('depends_on', {}).get('nodes', [])
      for dep in depends_on:
        if dep.startswith('model.'):
          # Extract model name from unique_id like "model.project.model_name"
          model_name = dep.split('.')[-1]

          if model_name not in self._test_index:
            self._test_index[model_name] = {}

          if column_name not in self._test_index[model_name]:
            self._test_index[model_name][column_name] = []

          self._test_index[model_name][column_name].append(test_name)

  def _init_models(self):
    if self._models == None:
      # Build test index first
      self._build_test_index()

      self._models = list(
        Model(node, self._test_index.get(node['name'], {})) for key, node in self.manifest['nodes'].items()
        if node['resource_type'] == 'model' and
        node['config']['materialized'] != 'ephemeral' and
        (any(node['path'].startswith(path) for path in self.paths) if self.paths else True) and
        all(tag in node['config']['tags'] for tag in self.tags) and
        (node['name'] in self.names if self.names else True)
      )
  
  @property
  def models(self) -> list[Model]:
    self._init_models()
    return self._models
  
  def model(self, name: str) -> Model:
    self._init_models()
    return next(model for model in self._models if model.name == name)
