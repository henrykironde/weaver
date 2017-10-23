from __future__ import print_function

import shutil

from weav.lib.models import *
from weav.engines import choose_engine
from weav.lib.defaults import DATA_DIR

from retriever import dataset_names
from weav.lib.templates import TEMPLATES


class Processor(object):
    def __init__(self, name=None, dictt =None):
        name = name
        config = dictt
        scripts = []

        scripts_needed = []


        for i in config["tables"]:
            scripts_needed.append(i["dataset"])
        print (set(scripts_needed))

        set()


