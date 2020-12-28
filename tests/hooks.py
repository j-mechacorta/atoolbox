import os
from os.path import dirname as _dir
import logging

def get_logger(name):
    return logging.getLogger('conftest.%s' % name)


def pytest_sessionstart(session):
    BASE_FORMAT = "[%(name)s][%(levelname)-6s] %(message)s"
    FILE_FORMAT = "[%(asctime)s]" + BASE_FORMAT

    root_logger = logging.getLogger('conftest')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    top_level = _dir(_dir(dir_path))
    log_file = os.path.join(top_level, 'pytest-functional-tests.log')
    print(log_file)

    root_logger.setLevel(logging.INFO)

    # File Logger
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FILE_FORMAT, "%Y-%m-%d %H:%M:%S"))

    root_logger.addHandler(fh)

