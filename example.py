import os
from modules.gs import Grapesaur
from argparse import ArgumentParser

if __name__ == "__main__":

    gs = Grapesaur(os.getcwd())
    gs.readFile("vericred_global_health_2021.json")
    # gs.showRows(vertical=True, no=1, truncate=False)
    # gs.showRows(no = 5, truncate = True, colname = 'addresses')
    print(gs.getColumnNames('fax'))
    gs.showFullSchema()
