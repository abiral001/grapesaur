import os
from modules.gs import Grapesaur
from argparse import ArgumentParser

if __name__ == "__main__":

    gs = Grapesaur(os.getcwd())
    gs.readFile("vericred_global_health_2021.json")
    # gs.showRows(10)
    # gs.showRows(colname = 'specialties', truncate=False, all=True)
    # print(gs.getColumnNames('addresses'))
    # gs.showUniqueData('site_uid')
    # print(gs.search('ABBAS', 'unparsed_name', 'networks, specialties, pcp'))
    gs.summary()
    # gs.tree()
