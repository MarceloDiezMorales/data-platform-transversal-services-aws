from sys import path
from os.path import abspath

def pytest_configure(config):
    path.insert(0, abspath("src"))