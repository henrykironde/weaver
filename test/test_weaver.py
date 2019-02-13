# # -*- coding: latin-1  -*-
# """Tests for the Data weaver"""
# from future import standard_library
#
# standard_library.install_aliases()
# import os
# import sys
# import subprocess
# from imp import reload
# from weaver.lib.defaults import ENCODING
#
# encoding = ENCODING.lower()
#
# reload(sys)
# if hasattr(sys, 'setdefaultencoding'):
#     sys.setdefaultencoding(encoding)
# import weaver as rt
# from weaver.lib.engine import Engine
# from weaver.lib.table import TabularDataset
# from weaver.lib.templates import BasicTextTemplate
# # from weaver.lib.cleanup import correct_invalid_value
# # from weaver.lib.engine_tools import getmd5
# # from weaver.lib.engine_tools import xml2csv
# # from weaver.lib.engine_tools import json2csv
# from weaver.lib.engine_tools import sort_file
# from weaver.lib.engine_tools import sort_csv
# from weaver.lib.engine_tools import create_file
# from weaver.lib.engine_tools import file_2list
# from weaver.lib.datapackage import clean_input, is_empty
#
# # Create simple engine fixture
# test_engine = Engine()
# test_engine.table = TabularDataset(**{"name": "test"})
# test_engine.script = BasicTextTemplate(
#     **{"tables": test_engine.table, "name": "test"})
# test_engine.opts = {'database_name': '{db}_abc'}
#
# # Main paths
# HOMEDIR = os.path.expanduser('~')
# FILE_LOCATION = os.path.dirname(os.path.realpath(__file__))
# weaver_root_dir = os.path.abspath(os.path.join(FILE_LOCATION, os.pardir))
#
# # Setup paths for the raw data files used
# raw_dir_files = os.path.normpath(os.path.join(weaver_root_dir,
#                                               'raw_data/{file_name}'))
#
#
# def setup_module():
#     """"Automatically sets up the environment before the module runs.
#
#     Make sure you are in the main local weaver directory.
#     """
#     os.chdir(weaver_root_dir)
#     subprocess.call(['cp', '-r', 'test/raw_data', weaver_root_dir])
#
#
# def teardown_module():
#     """Automatically clean up after the module.
#
#     Make sure you are in the main local weaver directory after these tests.
#     """
#     os.chdir(weaver_root_dir)
#     subprocess.call(['rm', '-r', 'raw_data'])
#     subprocess.call(['rm', '-r', test_engine.format_data_dir()])
#
#
# def setup_functions():
#     """Set up function.
#
#     Tests can use the function to clean up before running.
#     Not automatically run.
#     """
#     teardown_module()
#     setup_module()
#
#
#
#
# def test_create_db_statement():
#     """Test creating the create database SQL statement."""
#     assert test_engine.create_db_statement() == 'CREATE DATABASE test_abc'
#
#
# def test_database_name():
#     """Test creating database name."""
#     assert test_engine.database_name() == 'test_abc'
#
#
# def test_datasets():
#     """Check if datasets lookup includes a known value"""
#     datasets = rt.datasets(keywords=['mammals'])
#     dataset_names = [dataset.name for dataset in datasets]
#     assert 'mammal-masses' in dataset_names
#
#
# def test_datasets_keywords():
#     """Check if datasets lookup on keyword includes a known value"""
#     datasets = rt.datasets(keywords=['mammals'])
#     dataset_names = [dataset.name for dataset in datasets]
#     assert 'mammal-masses' in dataset_names
#
#
# def test_datasets_licenses():
#     """Check if datasets lookup on license includes a known value"""
#     datasets = rt.datasets(licenses=['CC0-1.0'])
#     dataset_names = [dataset.name for dataset in datasets]
#     assert 'amniote-life-hist' in dataset_names
#
#
# def test_dataset_names():
#     """Check if dataset names lookup includes a known value"""
#     assert 'mammal-masses' in rt.dataset_names()
#
#
# def test_drop_statement():
#     """Test the creation of drop statements."""
#     assert test_engine.drop_statement(
#         'TABLE', 'tablename') == "DROP TABLE IF EXISTS tablename"
#
#
# def test_find_file_absent():
#     """Test if find_file() properly returns false if no file is present."""
#     assert test_engine.find_file('missingfile.txt') is False
#
#
# def test_find_file_present():
#     """Test if existing datafile is found.
#
#     Using the bird-size dataset which is included for regression testing.
#     We copy the raw_data directory to weaver_root_dir
#     which is the current working directory.
#     This enables the data to be in the DATA_SEARCH_PATHS.
#     """
#     test_engine.script.name = 'bird-size'
#     assert test_engine.find_file('avian_ssd_jan07.txt') == os.path.normpath(
#         'raw_data/bird-size/avian_ssd_jan07.txt')
#
#
# def test_format_data_dir():
#     """Test if directory for storing data is properly formated."""
#     test_engine.script.name = "TestName"
#     r_path = '.weaver/raw_data/TestName'
#     assert os.path.normpath(test_engine.format_data_dir()) == \
#            os.path.normpath(os.path.join(HOMEDIR, r_path))
#
#
# def test_format_filename():
#     """Test if filenames for stored files are properly formated."""
#     test_engine.script.name = "TestName"
#     r_path = '.weaver/raw_data/TestName/testfile.csv'
#     assert os.path.normpath(test_engine.format_filename('testfile.csv')) == \
#            os.path.normpath(os.path.join(HOMEDIR, r_path))
#
#
# def test_format_insert_value_int():
#     """Test formatting of values for insert statements."""
#     assert test_engine.format_insert_value(42, 'int') == 42
#
#
# def test_sort_file():
#     """Test md5 sum calculation."""
#     data_file = create_file(['Ben,US,24', 'Alex,US,25', 'Alex,PT,25'])
#     out_file = sort_file(data_file)
#     obs_out = file_2list(out_file)
#     os.remove(out_file)
#     assert obs_out == ['Alex,PT,25', 'Alex,US,25', 'Ben,US,24']
#
#
# def test_sort_csv():
#     """Test md5 sum calculation."""
#     data_file = create_file(['User,Country,Age',
#                              'Ben,US,24',
#                              'Alex,US,25',
#                              'Alex,PT,25'])
#     out_file = sort_csv(data_file)
#     obs_out = file_2list(out_file)
#     os.remove(out_file)
#     assert obs_out == [
#         'User,Country,Age',
#         'Alex,PT,25',
#         'Alex,US,25',
#         'Ben,US,24']
#
#
# def test_is_empty_null_string():
#     """Test for null string."""
#     assert is_empty("")
#
#
# def test_is_empty_empty_list():
#     """Test for empty list."""
#     assert is_empty([])
#
#
# def test_is_empty_not_null_string():
#     """Test for non-null string."""
#     assert is_empty("not empty") == False
#
#
# def test_is_empty_not_empty_list():
#     """Test for not empty list."""
#     assert is_empty(["not empty"]) == False
#
#
# def test_setup_functions():
#     """Test the set up function.
#
#     Function uses teardown_module and setup_module functions."""
#     file_path = raw_dir_files.format(file_name='')
#     subprocess.call(['rm', '-r', file_path])
#     assert os.path.exists(raw_dir_files.format(file_name="")) is False
#     setup_functions()
#     assert os.path.exists(raw_dir_files.format(file_name=""))









# /Users/henrykironde/Documents/GitHub/weaver/version.py:
#     8
#     9
#    10: def write_version_file(scripts):
#    11      """The function creates / updates version.txt with the script version numbers."""
#    12      if os.path.isfile("version.txt"):
#    ..
#    19
#    20
#    21: def update_version_file():
#    22      """Update version.txt."""
#    23      scripts = get_script_version()

# /Users/henrykironde/Documents/GitHub/weaver/weaver/__main__.py:
#    20
#    21
#    22: def main():
#    23      """This function launches the weaver."""
#    24      if len(sys.argv) == 1:
#    ..
#   128
#   129
#   130: def print_info(all_scripts, keywords_license=False):
#   131      count = 1
#   132      for script in all_scripts:

# /Users/henrykironde/Documents/GitHub/weaver/weaver/compile.py:
#     5
#     6
#     7: def compile():
#     8      print("Compiling weaver scripts...")
#     9      reload_scripts()

# /Users/henrykironde/Documents/GitHub/weaver/weaver/engines/__init__.py:
#    15
#    16
#    17: def choose_engine(opts, choice=True):
#    18      """Prompts the user to select a database engine"""
#    19      if "engine" in list(opts.keys()):

# /Users/henrykironde/Documents/GitHub/weaver/weaver_engines_postgres.py:

#    37:     def create_db_statement(self):
#    44:     def create_db(self):
#    52:     def drop_statement(self, objecttype, objectname):
#    58:     def get_connection(self):

# /Users/henrykironde/Documents/GitHub/weaver/weaver_engines_sqlite.py:
#    32:     def create_db(self):
#    40:     def to_csv(self):
#    43:     def get_connection(self):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/cleanup.py:
#     2
#     delete this

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/datapackage.py:

#  delete of refactor for later

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/datasets.py:
#     4: def datasets(keywords=None, licenses=None):
#    36: def dataset_names():
#    47: def license(dataset):
#    52: def dataset_licenses():


# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/download.py:
# 	if we want to make it into to csv to get the data out of db like export
#    11: def download(dataset, path='./', quiet=False, subdir=False, debug=False):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/dummy.py:
#     6
#     7  class DummyConnection(object):
#     8:     def cursor(self):
#     9          pass
#    10
#    11:     def commit(self):
#    12          pass
#    13
#    14:     def rollback(self):
#    15          pass
#    16
#    17:     def close(self):
#    18          pass
#    19

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/engine.py:
#    42:     def connect(self, force_reconnect=False):
#    53:     def disconnect(self)
#    59:     def get_connection(self):
#    64:     def create_db(self):
#    84:     def create_db_statement(self):
#    89:     def create_raw_data_dir(self):
#    96:     def database_name(self, name=None):
#   109:     def download_file(self, url, filename):
#   127:     def drop_statement(self, objecttype, objectname):
#   132:     def execute(self, statement, commit=True):
#   138:     def executemany(self, statement, values, commit=True):
#   144:     def exists(self, script):
#   149:     def exists(self, database, table_name):
#   153:     def final_cleanup(self):
#   160:     def find_file(self, filename):
#   169:     def format_data_dir(self):
#   173:     def format_filename(self, filename):
#   177:     def get_cursor(self):
#   185:     def get_input(self):
#   202:     def set_engine_encoding(self):
#   205:     def set_table_delimiter(self, file_path):
#   210:     def table_exists(self, dbname, tablename):
#   215:     def table_name(self, name=None, dbname=None):
#   225:     def to_csv(self):
#   228:     def warning(self, warning):
#   232:     def load_data(self, filename)
#   254:     def extract_fixed_width(self, line):
#   263:     def gis_import(self, table):
#   267
#   275
#   276: def file_exists(path):
#   277      """Return true if a file exists and its size is greater than 0."""
#   278      return os.path.isfile(path) and os.path.getsize(path) > 0

#   299: def reporthook(count, block_size, total_size):
#   300      """Generate the progress bar.

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/engine_tools.py:
#    31: def create_home_dir():
#    53: def name_matches(scripts, arg):
#    88: def final_cleanup(engine):
#    96: def reset_weaver(scope="all", ask_permission=True):
#   214: def sort_file(file_path):
#   231: def sort_csv(filename):
#   269: def create_file(data, output='output_file'):
#   279: def file_2list(input_file):
#   294: def get_script_version():
#   313: def set_proxy():

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/excel.py:
# delete

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/install.py:
#    12: def _join(args, use_cache, debug, compile):
#    13      """Install scripts for weaver."""
#    14      engine = choose_engine(args)
#    38: def join_postgres(dataset, user='postgres', password='',
#    39                    host='localhost', port=5432, database='postgres',
#    40                    database_name=None, table_name=None,
#    67: def join_sqlite(dataset, file=None, table_name=None,
#    68                  compile=False, debug=False, quiet=False, use_cache=True):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/load_json.py:
#    21: def read_json(json_file, debug=False):
# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/process.py:
#     6: def rename_fields(dictionary_object, original, alias):

#    16: def make_sql(dataset):
# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/repository.py:

#    17: def _download_from_repository(filepath, newpath, repo=REPOSITORY):
#    29: def check_for_updates(quiet=False):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/scripts.py:
#    18: def reload_scripts()
#    44: def SCRIPT_LIST():
#    53: def get_script(dataset):
#    62: def open_fr(file_name, encoding=ENCODING, encode=True):
#    81: def open_fw(file_name, encoding=ENCODING, encode=True):
#    97: def open_csvw(csv_file, encode=True):
#   110: def to_str(object, object_encoding=sys.stdout):
#   121  class StoredScripts:

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/table.py:
#    16      all have some common table features
#    17      """
#    18:     def __init__(self, name=None):
#    19          self.name = name
#    20
#    ..
#    23      """Information about a database table."""
#    24
#    25:     def __init__(self, name=None,
#    26                   fields=[],
#    27                   table_type="tabular",
#    ..
#    45  class RasterDataset(Dataset):
#    46      """Raster table implementation"""
#    47:     def __init__(self, name=None,
#    48                   fields=[],
#    49                   table_type="raster",
#    ..
#    75      """Vector table implementation"""
#    76
#    77:     def __init__(self, name=None,
#    78                   fields=[],
#    79                   table_type="vector",

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/templates.py:
#    17      """
#    18
#    19:     def __init__(self, title="", description="", name="", urls=dict(),
#    20                   tables=dict(), ref="", public=True, addendum=None,
#    21                   citation="Not currently available",
#    ..
#    43              setattr(self, key, item[0] if isinstance(item, tuple) else item)
#    44
#    45:     def __str__(self):
#    46          desc = self.name
#    47          if self.reference_url():
#    ..
#    49          return desc
#    50
#    51:     def integrate(self, engine=None, debug=False):
#    52          """Generic function to prepare for integration."""
#    53          self.engine = self.checkengine(engine)
#    ..
#    56          self.engine.create_db()
#    57
#    58:     def reference_url(self):
#    59          if self.ref:
#    60              return self.ref
#    ..
#    65                  return None
#    66
#    67:     def checkengine(self, engine=None):
#    68          """Returns the required engine instance"""
#    69          if engine is None:
#    ..
#    74          return engine
#    75
#    76:     def exists(self, engine=None):
#    77          if engine:
#    78              return engine.exists(self)
#    ..
#    80              return False
#    81
#    82:     def matches_terms(self, terms):
#    83          try:
#    84              search_string = ' '.join([self.name,
#    ..
#   102      """
#   103
#   104:     def __init__(self, **kwargs):
#   105          Script.__init__(self, **kwargs)
#   106          for key in kwargs:
#   107              setattr(self, key, kwargs[key])
#   108
#   109:     def integrate(self, engine=None, debug=False, ):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/tools.py:
#    10: def open_fr(file_name, encoding=ENCODING, encode=True):
#    29: def open_fw(file_name, encoding=ENCODING, encode=True):
#    45: def open_csvw(csv_file, encode=True):
#    57: def to_str(object, object_encoding=sys.stdout):

# /Users/henrykironde/Documents/GitHub/weaver/weaver/lib/warning.py:
#     4  class Warning(object):
# /Users/henrykironde/Documents/GitHub/weaver/weaver/lscolumns.py:
#    12: def get_columns(values, cols):
#    28: def printls(values, max_width=None, spacing=2):




