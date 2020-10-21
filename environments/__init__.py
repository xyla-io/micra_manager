import json
import os
from typing import Dict, Optional

environment = {}

def get_environment(identifier: str) -> Dict[str, any]:
  current_dir = os.path.dirname(os.path.abspath(__file__))
  file_name = f'environment.{identifier}' if identifier else 'environment'
  with open(os.path.join(current_dir, f'{file_name}.json')) as f:
    return json.load(f)

def set_environment(identifier: Optional[str]=None):
  def get_identifier():
    if identifier is not None:
      return identifier
    try:
      return os.environ['MICRAENV']
    except KeyError:
      pass
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
      with open(os.path.join(root_dir, 'local_configure.json')) as f:
        return json.load(f)['environment']
    except FileNotFoundError:
      pass
    try:
      with open(os.path.join(root_dir,  'configure.json')) as f:
        return json.load(f)['environment']
    except FileNotFoundError:
      return ''
  identifier = get_identifier()

  global environment
  environment.clear()
  environment.update(get_environment(identifier=identifier))

set_environment()