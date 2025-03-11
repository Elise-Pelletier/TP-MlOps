import os
import yaml

# project root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
# full config path
CONFIG_PATH = os.path.join(ROOT_DIR, 'config.yml')

def get_full_path(rel_path):
    return os.path.normpath(os.path.join(ROOT_DIR, rel_path))

with open(CONFIG_PATH, "r") as f:

    # yaml to python dict
    CONFIG = yaml.load(f, Loader=yaml.SafeLoader)

    for key, value in CONFIG['paths'].items():
    # Ne pas modifier les URLs (on détecte si ça commence par 'http')
        if not value.startswith("http"):
            CONFIG['paths'][key] = get_full_path(value)