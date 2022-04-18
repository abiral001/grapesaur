import os
from modules.gs import Grapesaur
from argparse import ArgumentParser

if __name__ == "__main__":

    gs = Grapesaur(os.getcwd())
    gs.readFile("vericred_valenz_health_20220410.json")
    # gs.showRows(10)
    # gs.showRows(colname = 'provider_type', truncate=False, all=True)
    # print(gs.getColumnNames('provider'))
    # gs.showUniqueData('provider_type')
    print(gs.search('Paul', searchfield='unparsed_name'))
    # gs.summary()
    gs.tree()
