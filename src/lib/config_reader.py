import json
import sys

def read_config(name: str):
    return json.load(open(name))

if len(sys.argv) == 2:
    config = read_config(sys.argv[1])
else:
    config = read_config("config.json")