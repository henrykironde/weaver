# -*- coding: latin-1  -*-
# """Integrations tests for Data Weaver"""
from __future__ import print_function

import os
import shlex
import subprocess
import sys
from collections import OrderedDict
from imp import reload

import pandas
import pytest

from retriever import dataset_names
from retriever import install_postgres
from weaver import reload_scripts as weaver_reload_scripts
from weaver.engines import engine_list
from weaver.lib.defaults import ENCODING
from weaver.lib.load_json import read_json
from weaver.lib.engine_tools import getmd5

encoding = ENCODING.lower()

reload(sys)
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)

FILE_LOCATION = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
RETRIEVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.retriever/'))
RETRIEVER_DATA_DIR = os.path.normpath(
    os.path.expanduser('~/.retriever/raw_data/'))
RETRIEVER_SCRIPT_DIR = os.path.normpath(
    os.path.expanduser('~/.retriever/scripts/'))
WEAVER_HOME_DIR = os.path.normpath(os.path.expanduser('~/.weaver/'))
WEAVER_SCRIPT_DIR = os.path.normpath(os.path.expanduser('~/.weaver/scripts/'))

# Set postgres password, Appveyor service needs the password given
# The Travis service obtains the password from the config file.
os_password = ""
pgdbs = "localhost"
docker_or_travis = os.environ.get("IN_DOCKER")

# Check if the environment variable "IN_DOCKER" is set to "true"
if docker_or_travis == "true":
    os_password = 'Password12!'
    pgdbs = "pgdbs"

testdb = "testdb",
testschema = "testschema"

RETRIEVER_SPATIAL_DATA = [
    "breed_bird_survey",
    "bioclim",
    "mammal-community-db",
    "harvard-forest"
]
WEAVER_TEST_SCRIPTS = [
    ("breed-bird-routes-bioclim", "csvfile_name", "6fec0fc63007a4040d9bbc5cfcd9953e"),
    ("mammal-community-bioclim", "csvfile_name", "6fec0fc63007a4040d9bbc5cfcd9953e"),
    ("mammal-community-masses", "csvfile_name", "6fec0fc63007a4040d9bbc5cfcd9953e"),
    ("mammal-community-sites-bioclim", "csvfile_name", "6fec0fc63007a4040d9bbc5cfcd9953e"),
    ("mammal-community-sites-harvard-soil", "csvfile_name", "6fec0fc63007a4040d9bbc5cfcd9953e"),
]


# Weaver defaults
# Engines
postgres_engine, sqlite_engine = engine_list


# Weaver test data(Tuple)
# (Script file name with no extensio, script name, result table, expected)

# # File names without `.json` extension
# WEAVER_TEST_DATA_PACKAGE_FILES = [file_base_names[0]
#                                   for file_base_names in WEAVER_TEST_DATA]
# weaver_test_parameters = [(test[1], test[2], test[3])
#                           for test in WEAVER_TEST_DATA]


def file_exists(path):
    """Return true if a file exists and its size is greater than 0."""
    return os.path.isfile(path) and os.path.getsize(path) > 0


# Install RETRIEVER_SPATIAL_DATA:
def install_to_database(dataset, install_function, config):
    install_function(dataset, **config)
    return


def install_dataset_postgres(dataset):
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': pgdbs,
                            'port': 5432,
                            'database': 'testdb',
                            'database_name': 'testschema',
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    install_to_database(dataset, install_postgres, interface_opts)


def setup_postgres_retriever_db():
    # Install the requited retriever datasets
    for test_data in RETRIEVER_SPATIAL_DATA:
        install_dataset_postgres(test_data)


def teardown_postgres_db():
    # Retriever database
    cmd = 'psql -U postgres -d testdb -h ' + pgdbs + ' -w -c \"DROP SCHEMA IF EXISTS testschema CASCADE\"'
    subprocess.call(shlex.split(cmd))
    #
    # # Weaver database
    # for file_base_names in WEAVER_TEST_DATA:
    #     dataset = file_base_names[1]
    #     sql_stm = "DROP SCHEMA IF EXISTS " + dataset.replace("-", "_") + " CASCADE"
    #     cmd = 'psql -U postgres -d testdb -h ' + pgdbs + ' -w -c \"{sql_stm}\"'
    #     dfd = cmd.format(sql_stm=sql_stm)
    #     subprocess.call(shlex.split(dfd))


def setup_module():
    # set up postgres database
    setup_postgres_retriever_db()


# # Test Retriever resources
# def test_retriever_test_resources():
#     """Test retriever resource files"""
#     pass


# Weaver integration
def get_output_as_csv(dataset, engines, db):
    """integrate datasets and return the output as a csv."""
    import weaver
    weaver_reload_scripts()
    eng = weaver.join_postgres(dataset, database=testdb, host=pgdbs, password=os_password)
    csv_file = eng.to_csv()
    return csv_file


@pytest.mark.parametrize("dataset, csv_file, expected", WEAVER_TEST_SCRIPTS)
def test_postgres(dataset, csv_file, expected):
    tmpdir = None
    postgres_engine.opts = {'engine': 'postgres',
                            'user': 'postgres',
                            'password': os_password,
                            'host': pgdbs,
                            'port': 5432,
                            'database': testdb,
                            'database_name': testschema,
                            'table_name': '{db}.{table}'}
    interface_opts = {"user": 'postgres',
                      "password": postgres_engine.opts['password'],
                      "host": postgres_engine.opts['host'],
                      "database": postgres_engine.opts['database'],
                      "database_name": postgres_engine.opts['database_name'],
                      "table_name": postgres_engine.opts['table_name']}
    res_csv = get_output_as_csv(dataset, postgres_engine, db=postgres_engine.opts['database_name'])
    # assert getmd5(res_csv) ==expected
    assert file_exists(res_csv)
