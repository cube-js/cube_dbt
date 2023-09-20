import json

from urllib.request import urlopen
from cube_dbt.model import Model
    
class Dbt:
  def __init__(self, manifest: dict=None, path_prefix='', tags=[], names=[]) -> None:
    self.manifest = manifest
    self.path_prefix = path_prefix
    self.tags = tags
    self.names = names
    self._models = None
    pass

  def from_file(self, manifest_path: str) -> 'Dbt':
    with open(manifest_path, 'r') as file:
      self._load_manifest(file.read())
      return self

  def from_url(self, manifest_url: str) -> 'Dbt':
    with urlopen(manifest_url) as file:
      self._load_manifest(file.read())
      return self

  def _load_manifest(self, manifest_data: dict) -> None:
    self.manifest = json.loads(manifest_data)

  def _ensure_manifest_loaded(self) -> None:
    if self.manifest == None:
      raise RuntimeError('dbt manifest has not been loaded yet')

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
    self._ensure_manifest_loaded()
    self._init_models()
    return self._models
  
  def model(self, name) -> Model:
    self._ensure_manifest_loaded()
    self._init_models()
    return next(model for model in self._models if model.name == name)
