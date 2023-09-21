import json

from urllib.request import urlopen
from cube_dbt.model import Model
    
class Dbt:
  def __init__(self, manifest: dict) -> None:
    self.manifest = manifest
    self.path_prefix = ''
    self.tags = []
    self.names = []
    self._models = None
    pass

  @staticmethod
  def from_file(manifest_path: str) -> 'Dbt':
    with open(manifest_path, 'r') as file:
      manifest = json.loads(file.read())
      return Dbt(manifest)

  @staticmethod
  def from_url(manifest_url: str) -> 'Dbt':
    with urlopen(manifest_url) as file:
      manifest = json.loads(file.read())
      return Dbt(manifest)
    
  def filter(self, path_prefix='', tags=[], names=[]) -> 'Dbt':
    self.path_prefix = path_prefix
    self.tags = tags
    self.names = names
    return self

  def _init_models(self):
    if self._models == None:
      self._models = list(
        Model(node) for key, node in self.manifest['nodes'].items()
        if node['resource_type'] == 'model' and
        node['config']['materialized'] != 'ephemeral' and
        node['path'].startswith(self.path_prefix) and
        all(tag in node['config']['tags'] for tag in self.tags) and
        (node['name'] in self.names if self.names else True)
      )
  
  @property
  def models(self) -> list[Model]:
    self._init_models()
    return self._models
  
  def model(self, name) -> Model:
    self._init_models()
    return next(model for model in self._models if model.name == name)
