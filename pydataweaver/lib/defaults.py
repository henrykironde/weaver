import os

from pydataweaver._version import __version__

VERSION = __version__
COPYRIGHT = (
    "Copyright (C) 2015 The Data Weaver contributors and the University of Florida")
REPO_URL = "https://raw.github.com/weecology/pydataweaver/"
MAIN_BRANCH = REPO_URL + "main/"
REPOSITORY = MAIN_BRANCH
ENCODING = "ISO-8859-1"
HOME_DIR = os.path.expanduser("~/.pydataweaver/")
SCRIPT_SEARCH_PATHS = ["./", "scripts", os.path.join(HOME_DIR, "scripts/")]
SCRIPT_WRITE_PATH = SCRIPT_SEARCH_PATHS[-1]

# Create default data directory
DATA_DIR = "."
sample_script = """
"""
CITATION = """Not Available"""
