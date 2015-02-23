# magic loader
import json

try:
    with open('test.conf') as secret_config_data:
        globals().update(json.load(secret_config_data))
except IOError:
    pass

