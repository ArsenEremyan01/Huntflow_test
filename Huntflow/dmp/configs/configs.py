import os

import yaml

BASE_DIR = os.getenv("HUNTFLOW_PRJ_PATH")


def load_configs():
    file = os.path.join(BASE_DIR, 'dmp', 'configs', 'configs.yml')
    config = yaml.safe_load(open(file))
    return config
