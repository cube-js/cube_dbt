import json

from urllib.request import urlopen
from cube_dbt.model import Model, model_name

Manifest = dict

def load_dbt_manifest_from_file(manifest_path: str) -> 'Manifest':
  with open(manifest_path, 'r') as file:
    return json.loads(file.read())

def load_dbt_manifest_from_url(manifest_url: str) -> 'Manifest':
  with urlopen(manifest_url) as file:
    return json.loads(file.read())
    
def dbt_models(manifest: Manifest, path_prefix='', tags=[], names=[]) -> list[Model]:
  return list(
    node for key, node in manifest['nodes'].items()
    if node['resource_type'] == 'model' and
    node['config']['materialized'] != 'ephemeral' and
    node['path'].startswith(path_prefix) and
    all(tag in node['config']['tags'] for tag in tags) and
    (node['name'] in names if names else True)
  )
  
def dbt_model(models: list[Model], name: str) -> Model:
  return next(model for model in models if model_name(model) == name)