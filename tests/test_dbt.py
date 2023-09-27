import os 

from pytest import raises
from cube_dbt import Dbt

class TestDbt:
  def test_from_file(self):
    directory_path = os.path.dirname(os.path.realpath(__file__))
    dbt = Dbt.from_file(directory_path + '/manifest.json')
    model_names = list(model.name for model in dbt.models)
    assert model_names == [
      'users_copy',
      'orders_copy',
      'line_items_copy',
      'products_copy'
    ]

  def test_load_only_models(self):
    """
    Only load nodes with resource type 'model'
    """
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table'
          },
          'path': 'example/users_copy.sql'
        },
        'snapshot.jaffle_shop.snapshot_1': {
          'name': 'snapshot_1',
          'resource_type': 'snapshot'
        }
      }
    }
    dbt = Dbt(manifest)
    model_names = list(model.name for model in dbt.models)
    assert model_names == ['users_copy']

  def test_do_not_load_ephemeral_models(self):
    """
    Don't load models that are not materialized to the database
    """
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table'
          },
          'path': 'example/users_copy.sql'
        },
        'model.jaffle_shop.users_copy_2': {
          'name': 'users_copy_2',
          'resource_type': 'model',
          'config': {
            'materialized': 'view'
          },
          'path': 'example/users_copy_2.sql'
        },
        'model.jaffle_shop.users_copy_3': {
          'name': 'users_copy_3',
          'resource_type': 'model',
          'config': {
            'materialized': 'incremental'
          },
          'path': 'example/users_copy_3.sql'
        },
        'model.jaffle_shop.users_copy_4': {
          'name': 'users_copy_4',
          'resource_type': 'model',
          'config': {
            'materialized': 'ephemeral'
          },
          'path': 'example/users_copy_4.sql'
        }
      }
    }
    dbt = Dbt(manifest)
    model_names = list(model.name for model in dbt.models)
    assert model_names == [
      'users_copy',
      'users_copy_2',
      'users_copy_3'
    ]

  def test_load_models_by_path_prefix(self):
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table'
          },
          'path': 'example/users_copy.sql'
        },
        'model.jaffle_shop.users_copy_2': {
          'name': 'users_copy_2',
          'resource_type': 'model',
          'config': {
            'materialized': 'view'
          },
          'path': 'marts/users_copy_2.sql'
        }
      }
    }
    dbt = Dbt(manifest).filter(path_prefix='marts/')
    model_names = list(model.name for model in dbt.models)
    assert model_names == ['users_copy_2']

  def test_load_models_by_tag(self):
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table',
            'tags': [
              'cube'
            ]
          },
          'path': 'example/users_copy.sql'
        },
        'model.jaffle_shop.users_copy_2': {
          'name': 'users_copy_2',
          'resource_type': 'model',
          'config': {
            'materialized': 'view',
            'tags': []
          },
          'path': 'marts/users_copy_2.sql'
        }
      }
    }
    dbt = Dbt(manifest).filter(tags=['cube'])
    model_names = list(model.name for model in dbt.models)
    assert model_names == ['users_copy']

  def test_load_models_by_name(self):
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table'
          },
          'path': 'example/users_copy.sql'
        },
        'model.jaffle_shop.users_copy_2': {
          'name': 'users_copy_2',
          'resource_type': 'model',
          'config': {
            'materialized': 'view'
          },
          'path': 'marts/users_copy_2.sql'
        }
      }
    }
    dbt = Dbt(manifest).filter(names=['users_copy_2'])
    model_names = list(model.name for model in dbt.models)
    assert model_names == ['users_copy_2']

  def test_model(self):
    manifest = {
      'nodes': {
        'model.jaffle_shop.users_copy': {
          'name': 'users_copy',
          'resource_type': 'model',
          'config': {
            'materialized': 'table'
          },
          'path': 'example/users_copy.sql'
        },
        'model.jaffle_shop.users_copy_2': {
          'name': 'users_copy_2',
          'resource_type': 'model',
          'config': {
            'materialized': 'view'
          },
          'path': 'marts/users_copy_2.sql'
        }
      }
    }
    dbt = Dbt(manifest)
    assert dbt.model('users_copy_2').name == 'users_copy_2'