import os
from modules.gs import Grapesaur
from modules.__version__ import __version__
from argparse import ArgumentParser

if __name__ == "__main__":

    # gs = Grapesaur(os.getcwd())
    # gs.readFile("test.json")
    # # gs.showRows(10)
    # gs.showRows(colname = 'address_string', truncate=False, all=True)
    # print(gs.getColumnNames('addresses'))
    # gs.showUniqueData('provider_type')
    # print(gs.search('Paul', searchfield='unparsed_name'))
    # gs.summary()
    # gs.tree()

    #argparse
    parser = ArgumentParser(description="Grapesaur Data Analysis Tool v{}".format(__version__))
    parser.add_argument('-f', '--file', type=str, default='null', help="Specifies the file to process. REQUIRED")
    parser.add_argument('-d', '--display', type=str, const=10, nargs='?', help='Shows the first specified number of rows with specified columns from the set file. Default is 10.')
    parser.add_argument('-s', '--summary', action='store_true', help="Summarizes the entire set data")
    # implement the following:
    # 1. show unique rows
    # 2. get column names
    # 3. search specific data in specific column
    # 4. summarize
    # 5. tree view of the schema
    args = parser.parse_args()
    if (args.summary):
        print('summary')
    else:
        print(args.display)
        print('no summary')
    
