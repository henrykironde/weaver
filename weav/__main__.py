"""Data Retriever Wizard

Running this module directly will launch the download wizard, allowing the user
to choose from all scripts.

The main() function can be used for bootstrapping.

"""
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
from builtins import input
from imp import reload

from weav.engines import engine_list, choose_engine
from weav.lib.datapackage import create_json, edit_json, delete_json, get_script_filename
from weav.lib.datasets import datasets, license
from weav.lib.defaults import sample_script, CITATION, ENCODING
from weav.lib.get_opts import parser
from weav.lib.repository import check_for_updates
from weav.lib.scripts import SCRIPT_LIST
from weav.lib.tools import name_matches, reset_retriever

encoding = ENCODING.lower()
# sys removes the setdefaultencoding method at startup; reload to get it back
reload(sys)
if hasattr(sys, 'setdefaultencoding'):
    sys.setdefaultencoding(encoding)


def main():
    """This function launches the weav."""
    sys.argv[1:] = [arg.lower() for arg in sys.argv[1:]]
    if len(sys.argv) == 1:
        # if no command line args are passed, show the help options
        parser.parse_args(['-h'])

    else:

        args = parser.parse_args()

        if args.quiet:
            sys.stdout = open(os.devnull, 'w')

        if args.command == 'help':
            parser.parse_args(['-h'])

        # Citations and Ls are going to import from retriever

        if args.command == 'citation':
            if args.dataset is None:
                # get the citation of weaver
                print("ToDos::citation of weaver")
                return
            else:
                print("ToDos::the citation for dataset "+args.dataset )

        # list the data sets available
        if args.command == 'ls':
            print("ToDos:: list of all available data sets.\n "
                  "Either we search the default retriever data folder")
            return

        if args.command == 'join':
            # open the conf file
            print(args.config[0])
            from collections import OrderedDict
            import json
            json_object = OrderedDict()
            json_file = args.config[0]

            try:
                json_object = json.load(open(json_file, "r"))
            except ValueError:
                pass

        # get tables from scripts
        tables_items = json_object["tables"]



        # from weav.lib.scripts import MODULE_LIST
        # for items in MODULE_LIST():
        #     # print( items)
        #     print(json_object)

if __name__ == "__main__":
    main()
